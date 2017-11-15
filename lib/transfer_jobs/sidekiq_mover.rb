# frozen_string_literal: true
module TransferJobs
  class SidekiqMover
    # TODO(zen): expose batch api in Status, Locking, LockQueue and use that instead
    attr_reader :source, :dest, :logger, :progress
    cattr_accessor :shutdown

    def initialize(source:, dest:, logger:, progress:)
      @source = source
      @dest = dest
      @logger = logger
      @progress = progress # remove that
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
      locking_jobs = jobs_to_move.select { |job, _| job.item['unique'] }

      release_locks_in_source(locking_jobs)
      locked_jobs = acquire_locks_in_dest(locking_jobs)

      failed_to_lock = locking_jobs - locked_jobs
      jobs_to_append = jobs_to_move - failed_to_lock

      dest.append(jobs_to_append)
      recover_job_status(jobs_to_append)

      [jobs_to_append.size, failed_to_lock.size]
    end

    private

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

    class Locker
      include SidekiqUniqeJobs::OptionsWithFallback

      def initialize(item)
        @item = item
      end
    end

    def release_locks_in_source(jobs)
      jobs.each do |job, _|
        lock = Lock.new(job.item)
        lock.unlock(:client, source.redis)
      end
    end
  end
end
