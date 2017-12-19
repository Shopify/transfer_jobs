# frozen_string_literal: true
module TransferJobs
  class SidekiqMover < QueueMover
    def move_batch_to_dest(jobs_to_move)
      jobs_to_append, failed_to_lock = handle_locked_jobs(jobs_to_move)

      dest.append(jobs_to_append)

      [jobs_to_append.size, failed_to_lock.size]
    end

    private

    # We only support SidekiqUniqueJobs <= 5.0.10
    # If it isn't available provide a no-op version of `handle_locked_jobs`
    unless defined?(SidekiqUniqueJobs)
      def handle_locked_jobs(jobs_to_move)
        [jobs_to_move, []]
      end
    else
      # This class exists solely to grant access to the methods defined in
      # OptionsWithFallback, notable the `lock` method which builds to correct lock
      # for us to then acquire / release.
      class Locker
        include SidekiqUniqueJobs::OptionsWithFallback
        attr_reader :worker_class, :item
        def initialize(item, redis)
          @item = item
          @worker_class = item['class']
          @pool = ConnectionPool.new { redis }
        end

        def lock
          lock_class.new(item, @pool)
        end
      end

      def acquire_locks_in_dest(jobs)
        jobs.select do |job, _|
          locker = Locker.new(job.item, dest.redis)
          if with_sidekiq_redis(dest.redis) { locker.lock.lock(:client) }
            true
          else
            logger.warn "Dropping job_id=#{job.jid} with couldn't acquire lock with unique_key=#{locker.lock.unique_key}"
            false
          end
        end
      end

      def release_locks_in_source(jobs)
        jobs.each do |job, _|
          locker = Locker.new(job.item, source.redis)
          with_sidekiq_redis(source.redis) { locker.lock.unlock(:server) }
        end
      end

      def with_sidekiq_redis(redis)
        old_redis = ::Sidekiq.redis_pool
        ::Sidekiq.redis = ConnectionPool.new(size: 1) { redis }
        yield
      ensure
        ::Sidekiq.redis = old_redis
      end

      def handle_locked_jobs(jobs_to_move)
        locking_jobs = jobs_to_move.select { |job, _| job.item['unique'] }

        release_locks_in_source(locking_jobs)
        locked_jobs = acquire_locks_in_dest(locking_jobs)

        failed_to_lock = locking_jobs - locked_jobs
        jobs_to_append = jobs_to_move - failed_to_lock

        [jobs_to_append, failed_to_lock]
      end
    end
  end
end
