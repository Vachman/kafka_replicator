require 'multi_json'

namespace :kafka_replicator do
  desc 'Start topics replicator'
  task replicate_topics: :environment do |task, _|
    source_brokers = ENV['KAFKA_REPLICATOR_SOURCE_BROKERS'] && MultiJson.load(ENV['KAFKA_REPLICATOR_SOURCE_BROKERS'])
    raise "KAFKA_REPLICATOR_SOURCE_BROKERS environment variable is not set" unless source_brokers

    destination_brokers = ENV['KAFKA_REPLICATOR_DESTINATION_BROKERS'] && MultiJson.load(ENV['KAFKA_REPLICATOR_DESTINATION_BROKERS'])
    raise "KAFKA_REPLICATOR_DESTINATION_BROKERS environment variable is not set" unless destination_brokers

    skip_topics = (ENV['KAFKA_REPLICATOR_SKIP_TOPICS'] && MultiJson.load(ENV['KAFKA_REPLICATOR_SKIP_TOPICS'])) || []

    puts "Replicating from #{source_brokers} to #{destination_brokers}"
    puts "Skipping topics: #{(KafkaReplicator::TopicsReplicator::SKIP_TOPICS | skip_topics).sort}"

    replicator = KafkaReplicator::TopicsReplicator.new(
      source_brokers: source_brokers, 
      destination_brokers: destination_brokers,
      skip_topics: skip_topics
    )

    trap("TERM") { replicator.stop }
    trap("INT") { replicator.stop }
    
    replicator.start
  end
end
