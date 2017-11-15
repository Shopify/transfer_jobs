# frozen_string_literal: true
require 'redis_queue'
module TransferJobs
  class SidekiqQueue < RedisQueue
    def batch
      redis.lrange(key, @offset, @offset + @batch_size - 1).map { |job| decode(job) }
    end

    def append(jobs, queue: key)
      unless jobs.empty?
        serialized_jobs = jobs.map do |job|
          serialize(job)
        end

        redis.rpush(queue, serialized_jobs)
      end
    end

    def trim
      redis.ltrim(key, @batch_size, -1)
      @offset -= @batch_size
    end

    def size
      redis.llen(key)
    end

    private

    def serialize(job)
      job.value
    end

    def decode(queue_item)
      Sidekiq::Job.new(queue_item, key)
    end
  end
end
