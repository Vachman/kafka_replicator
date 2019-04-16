module KafkaReplicator
  class Railtie < Rails::Railtie
    rake_tasks do
      load 'tasks/kafka_replicator.rake'
    end
  end
end

