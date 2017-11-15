namespace :sidekiq do
  task :transfer do
    require 'logger'
    require 'redis'

    require 'transfer_jobs'
    require 'transfer_jobs/sidekiq'
    class NilProgress
      def tick(*)
      end
    end

    logger = Logger.new($stdout)
    progress = NilProgress.new

    unless ENV['SOURCE_URL']
      raise "Please provide a source url to transfer from"
    end

    unless ENV['DEST_URL']
      raise "Please provide a destination url to transfer to"
    end

    source_redis = Redis.new(url: ENV['SOURCE_URL'])
    dest_redis = Redis.new(url: ENV['DEST_URL'])

    def filter(job)
      true
    end

    multi_mover = TransferJobs::MultiQueueMover.new(
      source: TransferJobs::RedisJobSet.new(redis: source_redis),
      dest: TransferJobs::RedisJobSet.new(redis: dest_redis),
      logger: logger,
      progress: progress,
    )

    multi_mover.transfer do |queues|
      raise Interrupt if TransferJobs::SidekiqMover.shutdown
      queues.each do |queue, _|
        TransferJobs::SidekiqMover.new(
          source: TransferJobs::SidekiqQueue.new(redis: source.redis, key: source.queue_key(queue)),
          dest:   TransferJobs::SidekiqQueue.new(redis: dest.redis, key: dest.queue_key(queue)),
          logger: logger,
          progress: progress,
        ).transfer(&method(:filter))
      end
    end
  end
end
