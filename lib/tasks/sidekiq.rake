# frozen_string_literal: true

# This task is meant to be included inside of your application.

# It provides a `sidekiq:transfer` task to help you recover jobs from your
# sidekiq redis instance.

# Currently we can transfer jobs from sidekiq and sidekiq-unique-jobs. 
# These features are written so that they will activate based on available
# gems.

namespace :sidekiq do
  task :transfer do
    require 'logger'
    require 'redis'

    require 'transfer_jobs'
    require 'transfer_jobs/sidekiq'

    unless ENV['SOURCE_URL']
      raise "Please provide a source url to transfer from"
    end

    unless ENV['DEST_URL']
      raise "Please provide a destination url to transfer to"
    end

    source_redis = Redis.new(url: ENV['SOURCE_URL'])
    dest_redis = Redis.new(url: ENV['DEST_URL'])
    logger = Logger.new($stdout)

    def filter(job)
      true
    end

    include TransferJobs

    logger.info "Starting transfer of jobs from #{ENV['SOURCE_URL']} to #{ENV['DEST_URL']}"

    multi_mover = MultiQueueMover.new(
      source: RedisJobSet.new(redis: source_redis),
      dest: RedisJobSet.new(redis: dest_redis),
      logger: logger,
    )

    delayed_queue = SidekiqMover.new(
      source: SidekiqDelayedQueue.new(redis: source_redis),
      dest: SidekiqDelayedQueue.new(redis: dest_redis),
      logger: logger,
    )

    # https://github.com/mperham/sidekiq/blob/63ee43353bd3b753beb0233f64865e658abeb1c3/lib/sidekiq/api.rb#L641
    retry_queue = SidekiqMover.new(
      source: SidekiqDelayedQueue.new(key: 'retry', redis: source_redis),
      dest: SidekiqDelayedQueue.new(key: 'retry', redis: dest_redis),
      logger: logger,
    )

    # https://github.com/mperham/sidekiq/blob/63ee43353bd3b753beb0233f64865e658abeb1c3/lib/sidekiq/api.rb#L656
    dead_queue = SidekiqMover.new(
      source: SidekiqDelayedQueue.new(key: 'dead', redis: source_redis),
      dest: SidekiqDelayedQueue.new(key: 'dead', redis: dest_redis),
      logger: logger,
    )

    multi_mover.transfer do |queues|
      raise Interrupt if SidekiqMover.shutdown
      queues.each do |queue, _|
        SidekiqMover.new(
          source: SidekiqQueue.new(redis: source.redis, key: source.queue_key(queue)),
          dest:   SidekiqQueue.new(redis: dest.redis, key: dest.queue_key(queue)),
          logger: logger,
          ).transfer(&method(:filter))
      end
    end

    delayed_queue.transfer(&method(:filter))
    retry_queue.transfer(&method(:filter))
    dead_queue.transfer(&method(:filter))
  end
end
