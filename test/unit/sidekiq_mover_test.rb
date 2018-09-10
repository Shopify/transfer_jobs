require 'test_helper'
# frozen_string_literal: true
class TransferJobs::SidekiqMoverTest < TransferJobsTestCase
  include JobHelper

  def setup
    @source = Sidekiq.redis {|redis| redis }
    @dest = Redis.new(url: redis_url + "/1")
  end

  def teardown
    JobHelper.job_result_queue = []
    @dest.flushdb
    @source.flushdb
  end

  def test_transfers_locking_jobs
    LockedJob.perform_async(num: 1)

    work_off_jobs(@dest)
    assert_equal [], JobHelper.job_result_queue.map {|arg| arg[0]['num']}

    sidekiq_mover('default')

    work_off_jobs(@dest)

    assert_equal [1], JobHelper.job_result_queue.map {|arg| arg[0]['num']}
  end

  def test_checks_locks_in_target_dc
    LockedJob.perform_async(num: 1)

    with_sidekiq_redis(@dest) do
      LockedJob.perform_async(num: 2)
    end

    sidekiq_mover('default')

    work_off_jobs(@dest)
    assert_equal [2], JobHelper.job_result_queue.map {|arg| arg[0]['num']}
  end

  def test_acquires_lock_in_target_dc
    LockedJob.perform_async(num: 1)

    sidekiq_mover('default')

    with_sidekiq_redis(@dest) do
      LockedJob.perform_async(num: 2)
    end

    work_off_jobs(@dest)
    assert_equal [1], JobHelper.job_result_queue.map {|arg| arg[0]['num']}
  end

  def test_releases_locks_in_source_dc
    LockedJob.perform_async(num: 1)

    sidekiq_mover('default')

    LockedJob.perform_async(num: 2)

    work_off_jobs(@dest)
    assert_equal [1], JobHelper.job_result_queue.map {|arg| arg[0]['num']}

    work_off_jobs(@source)
    assert_equal [1, 2], JobHelper.job_result_queue.map {|arg| arg[0]['num']}
  end

  def test_can_move_retry_queue
    retry_queue_name = 'retry'

    CrashingJob.perform_async(num: 1)
    CrashingJob.perform_async(num: 2)

    2.times do
      begin
        work_off_jobs(@source)
      rescue CrashingJob::Error
      end
    end

    assert_equal 2, @source.zcard(retry_queue_name)

    sidekiq_delayed_mover(retry_queue_name)

    assert_equal 0, @source.zcard(retry_queue_name)
    assert_equal 2, @dest.zcard(retry_queue_name)
  end

  def test_can_move_dead_queue
    dead_queue_name = 'dead'

    CrashingJob.perform_async(num: 1)

    begin
      work_off_jobs(@source)
    rescue CrashingJob::Error
    end

    Timecop.freeze(Time.now + 60) do
      handle_delayed_jobs
      begin
        work_off_jobs(@source)
      rescue CrashingJob::Error
      end
    end

    assert_equal 1, @source.zcard(dead_queue_name)

    sidekiq_delayed_mover(dead_queue_name)

    assert_equal 0, @source.zcard(dead_queue_name)
    assert_equal 1, @dest.zcard(dead_queue_name)
  end

  private

  def sidekiq_delayed_mover(queue)
    TransferJobs::SidekiqMover.new(
      source: TransferJobs::SidekiqDelayedQueue.new(key: queue, redis: @source),
      dest: TransferJobs::SidekiqDelayedQueue.new(key: queue, redis: @dest),
      logger: Logger.new('/dev/null'),
    ).transfer { true }
  end

  def sidekiq_mover(queue)
    TransferJobs::SidekiqMover.new(
      source: TransferJobs::SidekiqQueue.new(redis: @source, key: "queue:#{queue}"),
      dest:   TransferJobs::SidekiqQueue.new(redis: @dest, key: "queue:#{queue}"),
      logger: Logger.new('/dev/null'),
    ).transfer { true }
  end
end
