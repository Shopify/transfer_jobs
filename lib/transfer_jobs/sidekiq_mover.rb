# frozen_string_literal: true
module TransferJobs
  class SidekiqMover
    # TODO(zen): expose batch api in Status, Locking, LockQueue and use that instead
    attr_reader :source, :dest, :logger, :progress

    def initialize(source:, dest:, logger:, progress:)
      @source = source
      @dest = dest
      @logger = logger
      @progress = progress # remove that
    end

    def self.shutdown=(bool)
      @shutdown = bool
    end

    def self.shutdown
      @shutdown
    end

    def transfer(&filter)
      logger.info "Resuming a transfer of '#{source.key}', re-run to move currently enqueued jobs" if source.recovery_already_exists?
      return 0 unless source.move_queue_to_recovery!

      logger.info "Recovering single queue. key=#{source.key}"
      num_moved = 0
      num_dropped = 0

      source.in_batches do |batch|
        raise Interrupt if self.class.shutdown

        jobs_to_move = batch.select do |job, _|
          progress.tick
          filter.call(job)
        end

        moved, dropped = move_batch_to_dest(jobs_to_move)

        source.redis.multi do
          source.reenqueue(batch - jobs_to_move)
          source.trim
        end

        num_moved += moved
        num_dropped += dropped
      end

      logger.info "Finished recovering single queue. queue_key=#{source.key}" \
        " num_moved=#{num_moved} num_dropped=#{num_dropped}"
      num_moved
    rescue StopIteration, Interrupt
      logger.error "Transfer of queue '#{@source.key}' was stopped" \
        " after transferring #{num_moved} jobs to the target redis"
      raise
    end

    def move_batch_to_dest(jobs_to_move)
      jobs_to_append, failed_to_lock = handle_locked_jobs(jobs_to_move)

      dest.append(jobs_to_append)

      [jobs_to_append.size, failed_to_lock.size]
    end

    private

    # We only support SidekiqUniqueJobs <= 5.0.5
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

        def initialize(item)
          @item = item
        end
      end

      def acquire_locks_in_dest(jobs)
        jobs.select do |job, _|
          lock = Locker.new(job.item)

          if lock.lock(:client, dest.redis)
            true
          else
            logger.warn "Dropping job_id=#{job.job_id} with lock owned by other_job_id=#{lock.owner_token}"
            false
          end
        end
      end

      def release_locks_in_source(jobs)
        jobs.each do |job, _|
          lock = Lock.new(job.item)
          lock.unlock(:client, source.redis)
        end
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
