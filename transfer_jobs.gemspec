Gem::Specification.new do |gem|
  gem.name          = "transfer_jobs"
  gem.version       = "0.0.0"
  gem.date          = "2017-11-15"
  gem.summary       = "Provides functionality to transfer and recover jobs from background worker libraries"
  gem.files         = `git ls-files | grep -Ev '^(test)'`.split("\n")
  gem.require_paths = ["lib"]
  gem.authors       = ["Xavier Denis"]

  gem.add_development_dependency "pry"
  gem.add_development_dependency "redis"
  gem.add_development_dependency "sidekiq"
  gem.add_development_dependency "bundler", "~> 1.7"
  gem.add_development_dependency "rake", "~> 10.0"
end
