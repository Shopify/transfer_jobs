module JobHelper
  class << self
    def job_result_queue=(other)
      @job_result_queue = other
    end

    def job_result_queue
      @job_result_queue ||= []
    end
  end


  class SimpleJob
    include Sidekiq::Worker
    def perform(*args)
      JobHelper.job_result_queue << args
    end
  end

  class LockedJob < SimpleJob
    sidekiq_options unique: :until_executed,
                    unique_args: ->(args) { [] }
  end

  class CrashingJob < SimpleJob
    class Error < StandardError ; end
    def perform(*)
      super
      raise Error, "This job crashes"
    end
  end

  class Mgr
    def options
      { queues: ['default'] }
    end
  end

  require 'sidekiq/processor'
  def work_off_jobs(redis)
    mgr = Mgr.new
    with_sidekiq_redis(redis) do
      processor = ::Sidekiq::Processor.new(mgr)
      while (job = processor.send :fetch)
        processor.send(:process, job)
      end
    end
  end

  def with_sidekiq_redis(redis)
    old_redis = Sidekiq.redis_pool
    Sidekiq.redis = redis.client.options
    yield
  ensure
    Sidekiq.redis = old_redis
  end
end
