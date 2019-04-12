module KafkaReplicator
  class TopicsReplicator
    attr_reader :source_kafka,
                :destination_kafka,
                :source_consumer,
                :destination_producer,
                :replicated_topics

    def initialize(source_brokers:, destination_brokers:)
      @source_brokers = source_brokers
      @destination_brokers = destination_brokers
      @replicated_topics = Set[]
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

    def source_consumer
      @source_consumer ||= source_kafka.consumer(group_id: "replicator")
    end

    def destination_producer
      @destination_producer ||= destination_kafka.producer
    end

    def start
      loop do
        puts 'Adding topics for replication...'
        subscribe_to_source_topics

        puts 'Strting replication...'
        replicate
      end

    rescue => e
      puts e
      puts e.cause.inspect
    end
    
    def stop
      source_consumer.stop
    end

    private

    def replicate
      source_consumer.each_batch(automatically_mark_as_processed: false) do |batch|
        puts "Received batch: #{batch.topic}/#{batch.partition}"
        puts 'New topics added, restarting...' && break unless new_topics.empty?

        batch.messages.each_slice(100).each do |messages|

          messages.each do |message|
            puts "m #{message.topic}/#{message.partition}/#{message.offset}"
            
            destination_producer.produce(
              message.value,
              topic: destination_topic(message.topic),
              partition: message.partition
            )
          end

          destination_producer.deliver_messages
          source_consumer.commit_offsets
        end
      end
    end

    def source_topics
      source_kafka.topics.select { |topic_name| topic_name.end_with?('replica') }.to_set
    end

    def new_topics
      source_topics - replicated_topics
    end

    def subscribe_to_source_topics
      destination_topics = destination_kafka.topics

      new_topics.each do |topic|
        source_consumer.subscribe(topic, start_from_beginning: false)
        replicated_topics << topic
        destination_topic_name = destination_topic(topic)

        unless destination_topics.include?(destination_topic_name)
          destination_kafka.create_topic(
            destination_topic_name,
            num_partitions: source_kafka.partitions_for(topic),
            replication_factor: 3 # Need to be specified because otherwise ruby-kafa driver will make it equal to 1
          )
        end

        puts "Topic added: #{topic}"
      end
    end

    def destination_topic(topic_name)
      topic_name.chomp('_replica')
    end
  end
end
