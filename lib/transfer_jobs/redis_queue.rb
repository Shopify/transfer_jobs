# frozen_string_literal: true
module TransferJobs
  class RedisQueue
    DEFAULT_BATCH_SIZE = 1000

    attr_reader :redis, :original_key, :key

    def initialize(redis:, key:, batch_size: DEFAULT_BATCH_SIZE)
      @redis = redis
      @original_key = key
      @key = key
      @offset = 0
      @batch_size = batch_size
    end

    def batch
      raise NotImplementedError
    end

    def append
      raise NotImplementedError
    end

    def trim
      raise NotImplementedError
    end

    def size
      raise NotImplementedError
    end

    # This will interrupt all processing for the affected queues
    def move_queue_to_recovery!
      recovery_key = "#{original_key}:recovery"
      redis.rename(key, recovery_key) unless recovery_already_exists?
      @key = recovery_key
    rescue Redis::CommandError => err
      raise unless err.message == "ERR no such key"
    end

    def recovery_already_exists?
      redis.exists("#{original_key}:recovery")
    end

    def reenqueue(jobs)
      append(jobs, queue: @original_key)
    end

    def in_batches
      until (items = batch).empty?
        yield items
        @offset += @batch_size
      end
    end
  end
end
