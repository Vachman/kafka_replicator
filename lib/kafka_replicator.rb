require "kafka"
require "kafka_replicator/offsets_sync"
require 'kafka_replicator/railtie' if defined?(Rails)
require "kafka_replicator/topics_replicator"
require "kafka_replicator/version"
require "logger"
require "multi_json"

module KafkaReplicator
  class Error < StandardError; end
end
