
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "kafka_replicator/version"

Gem::Specification.new do |spec|
  spec.name          = "kafka_replicator"
  spec.version       = KafkaReplicator::VERSION
  spec.authors       = ["Vachagan Gevorgyan"]
  spec.email         = ["v.gevorgyan@catawiki.nl"]

  spec.summary       = %q{Replicate topics from one kafka cluster to another}
  spec.description   = %q{Simple solution for organizing 2 way syncing between kafka clusters}
  spec.homepage      = "https://github.com/Vachman/kafka-replicator"
  spec.license       = "MIT"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.17"
  spec.add_development_dependency "rake", "~> 10.0"

  spec.add_dependency 'ruby-kafka', '~> 0.6.0'
  spec.add_dependency 'multi_json', '~> 1.0'
end
