# frozen_string_literal: true
module TransferJobs
  class RedisJobSet < RedisQueue
    def initialize(redis:, key: 'queues')
      super
    end

    def batch
      redis.smembers(key)
    end

    def append(items, queue: key)
      unless items.empty?
        redis.sadd(queue, items)
      end
    end

    def trim
      redis.del(key)
    end

    def queue_key(queue)
      "queue:#{queue}"
    end

    def size
      queues = batch
      redis.pipelined do
        queues.each do |name|
          redis.llen(queue_key(name))
        end
      end.reduce(:+)
    end
  end
end
