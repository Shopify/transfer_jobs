# frozen_string_literal: true
module TransferJobs
  class SidekiqDelayedQueue < SidekiqQueue
    # TODO(zen): add support upstream to get all keys and their estimated size
    SCHEDULER_TIMESTAMPS_KEY = "schedule"

    def initialize(key: SCHEDULER_TIMESTAMPS_KEY, **args)
      super(key: key, **args)
    end

    def batch
      redis.zrange(key, @offset, @offset + @batch_size - 1, with_scores: true).map do |job, timestamp|
        [decode(job), timestamp]
      end
    end

    def append(items, queue: key)
      unless items.empty?
        serialized_items = items.map do |job, timestamp|
          [timestamp, serialize(job)]
        end

        redis.zadd(queue, serialized_items)
      end
    end

    def trim
      redis.zremrangebyrank(key, 0, @batch_size - 1)
      @offset -= @batch_size
    end

    def size
      redis.zcard(key)
    end
  end
end
