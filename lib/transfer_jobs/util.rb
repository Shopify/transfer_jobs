module TransferJobs
  module Util
    extend self

    if defined?(Progressrus)
      def with_progress
        stores = Progressrus.stores.dup
        Progressrus.stores.clear
        Progressrus.stores << Progressrus::Store::Redis.new(dest_redis.redis)

        total = TransferJobs::RedisJobSet.new(redis: source_redis) + TransferJobs::SidekiqDelayedQueue.new(redis: source_redis).size
        progress = Progressrus.new(scope: :maintenance, name: "TransferJobs", total: total)
        logger.info "Progress for processing ~#{progress.total} entries is being logged."
        yield(progress)
      ensure
        progress.complete

        Progressrus.stores.clear
        stores.each do |progress_store|
          Progressrus.stores << progress_store
        end
      end
    else
      class NilProgress
        def tick(*)
        end
      end

      def with_progress
        yield(NilProgress.new)
      end
    end
  end
end
