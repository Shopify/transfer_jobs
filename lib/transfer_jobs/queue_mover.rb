module TransferJobs
  class QueueMover
    attr_reader :source, :dest, :logger, :progress

    def initialize(source:, dest:, logger:, progress:)
      @source = source
      @dest = dest
      @logger = logger
      @progress = progress
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

    # This method provides the functionality to actually move a batch of jobs to the destination redis
    # This includes functionality like acquiring and releasing locks.

    def move_batch_to_dest
      raise NotImplementedError
    end
  end
end
