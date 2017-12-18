# Transfer Jobs

TransferJobs is a gem providing components and tasks to recover from failure of redis delayed job queues. We provide out-of-the-box support for common Sidekiq and Resque configurations as well as the tools you need to assemble a more complex recovery flow.

## Installation

To install `transfer_jobs` simply add `gem 'transfer_jobs', source: xxxx` to your `Gemfile`. This will provide access to all the classes and components of the gem.

To use the rake tasks you will need to include the relevant task in your `Rakefile` by adding:

Sidekiq:
```rake
import 'transfer_jobs/tasks/sidekiq.rake'

```

Resque:
```rake
# not yet implemented
import 'transfer_jobs/tasks/resquerake'

```

The rake tasks have secondary dependencies implied by their names. They are detailed in the section relevant to your job system.

### Optional Dependencies

There are several optional dependencies that enable quality-of-life features.

- Progresrus: Including the `progressrus` gem enables progress tracking during your transfers.

## Resque

TBD

## Sidekiq

TransferJobs supports common Sidekiq job configurations. The provided classes and modules should enable the construction of more comlex or atypical flows. The provided `sidekiq:transfer` rake task supports the following out-of-the-box:

- sidekiq ~> 5.0
- sidekiq-unique-jobs ~> 5.0.5
- sidekiq-scheduler ?

### Caveats

- https://github.com/mperham/sidekiq/wiki/Pro-Expiring-Jobs
- https://github.com/mperham/sidekiq/wiki/Pro-Reliability-Client
- https://github.com/mperham/sidekiq/wiki/Pro-Reliability-Server
- https://github.com/mperham/sidekiq/wiki/Reliability
- https://github.com/mperham/sidekiq/wiki/Ent-Periodic-Jobs

## Usage

Transfers can be initiated by running the relevant rake task with the correct parameters.

For example, to transfer a sidekiq application's tasks you would run:

```bash
bundle exec rake sidekiq:transfer SOURCE=redis://facebook-commerce.railgun/0 SOURCE=redis://facebook-commerce.railgun/1
```

This will transfer all jobs from the `0` db to the `1` db on the redis host `facebook-commerce.railgun`

## Failure

Just as important as knowing how to run your job transfers, understanding how they fail is key to ensuring the consistency of your jobs.

To prevent race conditions and double performs / enqueues, `transfer_jobs` relies heavily on renaming objects in Redis. When we start transferring a job queue we begin by performing a `RENAME`, on that queue, appending `:recovery` to the name. This transforms a `normal` queue to `normal:recovery`. This effectively hides those jobs from your job processing system, allowing us to transfer them in peace.

If a previous transfer was interrupted, a recovery queue will have been left behind. When `transfer_jobs` is run again it will detect that existing recovery queue and resume the previous transfer. However it **will not** rename the _existing_ queue. That means you will need to run `transfer_jobs` for a **third time**.

The other major risk is that a transfer is killed or interrupted midway through. Internally, we take measures to make transfers safe to interrupt, watching for signals that would indicate an exit and cleaning things up. However, there is always the possibility of uncontrolled-exit. When this happens there is potential for the batch of jobs currently being transferred to be _duplicated_ in the target datacenter.

