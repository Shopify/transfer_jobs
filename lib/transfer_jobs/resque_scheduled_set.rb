# frozen_string_literal: true
module TransferJobs
  class ResqueScheduledSet < RedisQueue
    # TODO(zen): add support upstream to get all keys and their estimated size
    SCHEDULER_TIMESTAMPS_KEY = "delayed_queue_schedule"

    def initialize(key: SCHEDULER_TIMESTAMPS_KEY, **args)
      super(key: key, **args)
    end

    def batch
      redis.zrange(key, @offset, @offset + @batch_size - 1, with_scores: true)
    end

    def append(items, queue: key)
      unless items.empty?
        redis.zadd(queue, items.map(&:reverse))
      end
    end

    def trim
      redis.zremrangebyrank(key, 0, @batch_size - 1)
      @offset -= @batch_size
    end

    def queue_key(queue)
      "delayed:#{queue}"
    end

    def size
      redis.zcard(key)
    end
  end
end
