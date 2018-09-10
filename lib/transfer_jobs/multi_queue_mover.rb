# frozen_string_literal: true
module TransferJobs
  class MultiQueueMover
    attr_reader :source, :dest, :logger

    # Abstracts over iteration of all resque queues:
    # For resque this could be {low, maintennace, payment-pod1, ...}
    # For scheduled jobs, this could be {scheduled:1, schedueld:38293892, ...}
    def initialize(source:, dest:, logger:)
      @source = source
      @dest = dest
      @logger = logger
    end

    def transfer(&transfer)
      msg = "Resuming a transfer of #{source.size} queues at '#{source.key}', re-run to move currently enqueued jobs"
      logger.info msg if source.recovery_already_exists?
      return 0 unless source.move_queue_to_recovery!

      logger.info "Recovering jobs from queues at '#{source.key}' num_queues=#{source.size}"
      source.in_batches do |queues|
        instance_exec(queues, &transfer)

        dest.append(queues)
        source.redis.multi do
          source.reenqueue(queues)
          source.trim
        end
      end

      logger.info "Finished recovery of queues at '#{source.key}'"
    rescue StopIteration, Interrupt
      logger.info "Recovery of job queues aborted."
    end
  end
end
