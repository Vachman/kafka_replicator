module KafkaReplicator
  class OffsetsSync
    attr_reader :source_kafka,
                :destination_kafka,
                :destination_consumer,
                :consumer_group,
                :logger,
                :topics

    def initialize(source_brokers:, destination_brokers:, consumer_group:)
      @source_brokers = source_brokers
      @destination_brokers = destination_brokers
      @consumer_group = consumer_group
      @topics = Hash.new { |h, k| h[k] = {} }
      @logger = Logger.new(STDOUT)
    end

    def source_kafka
      @source_kafka ||= Kafka.new(
        @source_brokers,
        client_id: "replicator_source"
      )
    end

    def destination_kafka
      @destination_kafka ||= Kafka.new(
        @destination_brokers,
        client_id: "replicator_destination"
      )
    end

    def destination_consumer
      @destination_consumer ||= destination_kafka.consumer(
        group_id: consumer_group
      )
    end

    def source_group_cordinator
      source_kafka.instance_variable_get('@cluster').send(
        :get_group_coordinator,
        group_id: consumer_group
      )
    end

    def source_consumer_offsets
      Kafka::Protocol::OffsetFetchRequest.send(:define_method, "api_version") { 2 }
      source_group_cordinator.fetch_offsets(group_id: consumer_group, topics: nil)
    end

    def load_source_consumer_offsets
      logger.info "load_source_consumer_offsets"

      source_consumer_offsets.topics.each do |topic, partitions|
        partitions.map do |partition, info|
          @topics[topic][partition] = { source_consumer_offset: info.offset }
        end
      end
    end

    def load_source_producer_offsets
      logger.info "load_destination_producer_offsets"

      source_kafka.last_offsets_for(*@topics.keys).each do |topic, partitions|
        partitions.each do |partition, offset|
          @topics[topic][partition][:source_producer_offset] = offset
        end
      end
    end

    def load_destination_producer_offsets
      logger.info "load_source_producer_offsets"

      destination_kafka.last_offsets_for(*@topics.keys).each do |topic, partitions|
        partitions.each do |partition, offset|
          @topics[topic][partition][:destination_producer_offset] = offset
        end
      end
    end

    def calculate_destination_consumer_offsets
      logger.info "calculate_destination_consumer_offsets"

      @topics.each do |topic, partitions|
        partitions.each do |partition, info|
          delta = info[:source_producer_offset] - info[:destination_producer_offset]
          info[:destination_consumer_offsets] = info[:source_consumer_offset] - delta
        end
      end
    end

    def set_destination_consumer_offsets
      logger.info "set_destination_consumer_offsets"

      @topics.each do |topic, partitions|
        destination_consumer.subscribe(topic)
        partitions.each do |partition, info|
          offset = info[:destination_consumer_offsets]
          logger.info "Seeking consumer offset for: #{m.topic}/#{m.partition} to #{offset}"
          destination_consumer.seek(topic, partition, offset)
        end
      end

      opts = { automatically_mark_as_processed: false }
      destination_consumer.each_message(opts) do |m|
        logger.info "Setting consumer offset for: #{m.topic}/#{m.partition}"
        break
      end
    end

    def sync
      load_source_consumer_offsets
      load_destination_producer_offsets
      load_source_producer_offsets

      calculate_destination_consumer_offsets
      set_destination_consumer_offsets
    end
  end
end
