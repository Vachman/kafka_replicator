require "kafka_replicator/offsets_sync"
require "kafka_replicator/topics_replicator"
require "kafka_replicator/version"
require "kafka"

module KafkaReplicator
  class Error < StandardError; end
end
