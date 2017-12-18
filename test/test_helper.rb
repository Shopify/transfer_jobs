require 'bundler/setup'

Bundler.require(:default, :test, :development)

require 'minitest/autorun'
require 'minitest/unit'

require 'transfer_jobs'
require 'transfer_jobs/sidekiq'

Dir[File.join(File.expand_path("../support/**/*.rb", __FILE__))].each do |support|
  require support
end

def redis_url
  ENV['CI'] ? 'redis://127.0.0.1' : 'redis://transfer-jobs.railgun'
end

Sidekiq.configure_client do |config|
  config.redis = { url: redis_url }
end

Sidekiq.configure_server do |config|
  config.redis = { url: redis_url }
end

Sidekiq::Logging.logger = nil

class TransferJobsTestCase < Minitest::Test
  def teardown
    Sidekiq.redis {|redis| redis.flushall }
  end
end

# should probably split test suite into 2, one for sidekiq one for resque.

# there should probs be 2 test-helper classes, one for sidekiq one for resque

