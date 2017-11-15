# frozen_string_literal: true
module TransferJobs
  class ResqueQueue < RedisQueue
    def batch
      redis.lrange(key, @offset, @offset + @batch_size - 1).map { |job| decode(job) }
    end

    def append(jobs, queue: key)
      unless jobs.empty?
        # Taken from Resque::Job#create
        # Resque does not provide an api which does the complete serialization of a job for redis
        # Instead, it performs the serialization in the same step that enqueues the job to redis
        # To complicate matters further ResqueScheduler relies on an extra `queue` parameter.
        # Safety of this code is guaranteed by the test suite of TransferJobs which verifies that
        # jobs can both be enqueued and dequeued.
        # TODO(zen): expose serialization api in Resque / ResqueScheduler

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
      { class: job.class.to_s, args: [job.serialize], queue: job.queue_name, nonce: rand }.to_json
    end

    def decode(queue_item)
      payload = Resque.decode(queue_item)
      job_name = payload['class']
      job_name.constantize.deserialize(payload['args'].first)
    end
  end
end
