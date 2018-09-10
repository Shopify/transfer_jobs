# frozen_string_literal: true
module TransferJobs
  class QueueMover
    # TODO(zen): expose batch api in Status, Locking, LockQueue and use that instead
    attr_reader :source, :dest, :logger
    cattr_accessor :shutdown

    def initialize(source:, dest:, logger:)
      @source = source
      @dest = dest
      @logger = logger
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
      locking_jobs = jobs_to_move.select { |job, _| job.try :lock_key }

      lock_queue_jobs = locking_jobs.select { |job, _| job.is_a?(BackgroundQueue::LockQueue) }
      recover_lock_queues(lock_queue_jobs)

      release_locks_in_source(locking_jobs)
      locked_jobs = acquire_locks_in_dest(locking_jobs)

      failed_to_lock = locking_jobs - locked_jobs
      jobs_to_append = jobs_to_move - failed_to_lock

      dest.append(jobs_to_append)
      recover_job_status(jobs_to_append)

      [jobs_to_append.size, failed_to_lock.size]
    end

    private

    def with_queue_redis(queue)
      old = Resque.redis
      old_bq = BackgroundQueue.redis
      BackgroundQueue.redis = queue.redis.redis
      Resque.redis = queue.redis
      yield
    ensure
      BackgroundQueue.redis = old_bq
      Resque.redis = old
    end

    def recover_lock_queues(lock_queue_jobs)
      lock_queue_jobs.each do |job, _|
        source_lock_queue = with_queue_redis(source) do
          job.lock_queue_instance
        end

        dest_lock_queue = with_queue_redis(dest) do
          job.lock_queue_instance
        end

        source_lock_queue.rename("#{job.lock_queue_key}:recovery")

        source_lock_queue.each do |queue_job|
          dest_lock_queue.push([queue_job].to_json)
        end
      end
    end

    def acquire_locks_in_dest(jobs)
      jobs.select do |job, _|
        lock = job.send(:build_lock, dest.redis)

        if lock.acquire(job.lock_timeout, fallback: RedisLocking::Lock::NO_FALLBACK)
          true
        else
          logger.warn "Dropping job_id=#{job.job_id} with lock owned by other_job_id=#{lock.owner_token}"
          false
        end
      end
    end

    def release_locks_in_source(jobs)
      jobs.each do |job, _|
        lock = job.send(:build_lock, source.redis)
        lock.have_lock = true
        lock.release
      end
    end

    # TODO(zen): Push this into the Status::Indexed. Add support for batch writes to Status
    def recover_job_status(jobs)
      jobs = jobs.select { |job, _| job.is_a?(BackgroundQueue::Status) }.map { |job, _| job }
      return if jobs.empty?

      job_status = BackgroundQueue::Status.fetch_multi(jobs.map(&:job_id), redis: source.redis.redis)

      jobs_by_shop = jobs.group_by(&:shop_id)

      jobs_by_shop.each do |shop_id, shop_jobs|
        dest.redis.redis.pipelined do
          shop_jobs.each do |job|
            BackgroundQueue::Status.write(
              job.job_id,
              job_status[job.job_id],
              expires_in: job.class.job_status_tll,
              redis: dest.redis.redis,
            )
          end
        end

        recover_indexed_job_status(shop_id, shop_jobs)
      end
    end

    def recover_indexed_job_status(shop_id, jobs)
      indexed_jobs = jobs.select { |job| job.is_a?(BackgroundQueue::Status::Indexed) }

      indexed_jobs.group_by(&:class).each do |job_class, class_jobs|
        job_ids = class_jobs.map(&:job_id)

        index = job_class.index_for_shop_id(shop_id)
        index.add(job_ids, expires_at: Time.now.to_i + job_class.job_status_tll, gc: false)
      end
    end
  end
end
