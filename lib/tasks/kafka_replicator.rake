require 'json'

namespace :kafka_replicator do
  desc 'Start topics replicator'
  task replicate_topics: :environment do |task, _|
    source_brokers = JSON.parse(ENV['KAFKA_REPLICATOR_SOURCE_BROKERS'])
    raise "KAFKA_REPLICATOR_SOURCE_BROKERS environment variable is not set" unless source_brokers

    destination_brokers = JSON.parse(ENV['KAFKA_REPLICATOR_DESTINATION_BROKERS'])
    raise "KAFKA_REPLICATOR_DESTINATION_BROKERS environment variable is not set" unless destination_brokers

    puts "Replicating from #{source_brokers} to #{destination_brokers}"

    KafkaReplicator::TopicsReplicator.new(
      source_brokers: source_brokers, 
      destination_brokers: destination_brokers
    ).start 
  end
end
