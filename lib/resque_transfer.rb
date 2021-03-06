class ResqueTransfer
  DEFAULT_BLACKLIST = [
    BugsnagDeliveryJob,
  ]

  AlreadyRunning = Class.new(StandardError)

  attr_reader :progress, :source_redis, :dest_redis, :logger

  cattr_accessor(:lock_wait_time) { 1.minute }

  def initialize(source_redis, dest_redis: Resque.redis, logger: Rails.logger, pod_id: nil, blacklist: DEFAULT_BLACKLIST)
    @source_redis = source_redis
    @dest_redis = dest_redis
    @blacklist = Set.new(blacklist.map(&:to_s))
    @logger = logger
    @pod_id = pod_id
  end

  def recover
    with_lock(source_redis) do
      with_progress do |progress|
        recover_queued_jobs(progress).transfer(&method(:queue_transfer))
        recover_old_scheduled_jobs(progress).transfer(&method(:filter))
        recover_new_scheduled_jobs(progress).transfer(&method(:filter))
      end
    end
  end

  def self.shutdown!
    QueueMover.shutdown = true
  end

  private

  def queue_transfer(queues)
    raise Interrupt if QueueMover.shutdown
    queues.each do |queue, _|
      QueueMover.new(
        source: ResqueQueue.new(redis: source.redis, key: source.queue_key(queue)),
        dest:   ResqueQueue.new(redis: dest.redis, key: dest.queue_key(queue)),
        logger: logger,
        progress: progress,
      ).transfer(&method(:filter))
    end
  end

  def recover_queued_jobs(progress)
    MultiQueueMover.new(
      source: RedisJobSet.new(redis: source_redis),
      dest: RedisJobSet.new(redis: dest_redis),
      logger: logger,
      progress: progress,
    )
  end

  def recover_old_scheduled_jobs(progress)
    MultiQueueMover.new(
      source: RedisScheduledSet.new(redis: source_redis),
      dest: RedisScheduledSet.new(redis: dest_redis),
      logger: logger,
      progress: progress,
    )
  end

  def recover_new_scheduled_jobs(progress)
    QueueMover.new(
      source: ResqueDelayedQueue.new(redis: source_redis),
      dest:   ResqueDelayedQueue.new(redis: dest_redis),
      logger: logger,
      progress: progress
    )
  end

  def with_progress
    stores = Progressrus.stores.dup
    Progressrus.stores.clear
    Progressrus.stores << Progressrus::Store::Redis.new(dest_redis.redis)

    total = RedisJobSet.new(redis: source_redis).size + RedisScheduledSet.new(redis: source_redis).size + ResqueDelayedQueue.new(redis: source_redis).size
    progress = Progressrus.new(scope: :maintenance, name: "TransferJobs", total: total)
    logger.info "Progress for processing ~#{progress.total} entries is logged to https://app.shopify.com/services/internal/background_job_progress"
    yield(progress)
  ensure
    progress.complete

    Progressrus.stores.clear
    stores.each do |progress_store|
      Progressrus.stores << progress_store
    end
  end

  def with_lock(redis, &block)
    unless RedisLocking::Lock.acquire(lock_key, lock_timeout, wait: TransferJobs.lock_wait_time, redis: redis, &block)
      raise AlreadyRunning, "An instance of transfer jobs is already running from #{redis.id}"
    end
  end

  # It's never safe to run this concurrently on a redis.
  def lock_key
    "transfer_jobs"
  end

  def lock_timeout
    30.minutes
  end

  def filter(job)
    return false if @blacklist.include?(job.class.name)
    return true if !@pod_id || @pod_id == 'all'

    if @pod_id == 'master'
      master_job_from_master_context?(job)
    else
      job.pod_id == @pod_id
    end
  end

  def master_job_from_master_context?(job)
    return false if job.class.include?(Podding::BackgroundQueue::SelectPod)
    job.pod_id.nil?
  end
end

