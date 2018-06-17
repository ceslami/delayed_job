require 'timeout'
require 'active_support/dependencies'
require 'active_support/core_ext/numeric/time'
require 'active_support/core_ext/class/attribute_accessors'
require 'active_support/hash_with_indifferent_access'
require 'active_support/core_ext/hash/indifferent_access'
require 'logger'
require 'benchmark'
require 'concurrent'

module Delayed
  class Worker # rubocop:disable ClassLength
    DEFAULT_LOG_LEVEL        = 'info'.freeze
    DEFAULT_SLEEP_DELAY      = 5
    DEFAULT_MAX_ATTEMPTS     = 25
    DEFAULT_MAX_RUN_TIME     = 4.hours
    DEFAULT_DEFAULT_PRIORITY = 0
    DEFAULT_DELAY_JOBS       = true
    DEFAULT_QUEUES           = [].freeze
    DEFAULT_QUEUE_ATTRIBUTES = HashWithIndifferentAccess.new.freeze
    DEFAULT_READ_AHEAD       = 5
    DEFAULT_QUIET            = true
    DEFAULT_POOL_SIZE        = 1

    cattr_accessor :min_priority, :max_priority, :max_attempts, :max_run_time,
                   :default_priority, :sleep_delay, :logger, :delay_jobs, :queues,
                   :read_ahead, :plugins, :destroy_failed_jobs, :exit_on_complete,
                   :default_log_level, :quiet, :pool_size

    # Named queue into which jobs are enqueued by default
    cattr_accessor :default_queue_name

    cattr_reader :backend, :queue_attributes

    def self.reset
      self.default_log_level = DEFAULT_LOG_LEVEL
      self.sleep_delay       = DEFAULT_SLEEP_DELAY
      self.max_attempts      = DEFAULT_MAX_ATTEMPTS
      self.max_run_time      = DEFAULT_MAX_RUN_TIME
      self.default_priority  = DEFAULT_DEFAULT_PRIORITY
      self.delay_jobs        = DEFAULT_DELAY_JOBS
      self.queues            = DEFAULT_QUEUES
      self.queue_attributes  = DEFAULT_QUEUE_ATTRIBUTES
      self.read_ahead        = DEFAULT_READ_AHEAD
      self.quiet             = DEFAULT_QUIET
      self.pool_size         = DEFAULT_POOL_SIZE
      @lifecycle             = nil
    end

    # Add or remove plugins in this list before the worker is instantiated
    self.plugins = []

    # By default failed jobs are destroyed after too many attempts. If you want to keep them around
    # (perhaps to inspect the reason for the failure), set this to false.
    self.destroy_failed_jobs = true

    def self.backend=(backend)
      if backend.is_a? Symbol
        require "delayed/serialization/#{backend}"
        require "delayed/backend/#{backend}"
        backend = "Delayed::Backend::#{backend.to_s.classify}::Job".constantize
      end
      @@backend = backend # rubocop:disable ClassVars
      silence_warnings { ::Delayed.const_set(:Job, backend) }
    end

    # rubocop:disable ClassVars
    def self.queue_attributes=(val)
      @@queue_attributes = val.with_indifferent_access
    end

    def self.before_fork
      unless @files_to_reopen
        @files_to_reopen = []
        ObjectSpace.each_object(File) do |file|
          @files_to_reopen << file unless file.closed?
        end
      end

      backend.before_fork
    end

    def self.after_fork
      # Re-open file handles
      @files_to_reopen.each do |file|
        begin
          file.reopen file.path, 'a+'
          file.sync = true
        rescue ::Exception # rubocop:disable HandleExceptions, RescueException
        end
      end
      backend.after_fork
    end

    def self.lifecycle
      # In case a worker has not been set up, job enqueueing needs a lifecycle.
      setup_lifecycle unless @lifecycle

      @lifecycle
    end

    def self.setup_lifecycle
      @lifecycle = Delayed::Lifecycle.new
      plugins.each { |klass| klass.new }
    end

    def self.delay_job?(job)
      if delay_jobs.is_a?(Proc)
        delay_jobs.arity == 1 ? delay_jobs.call(job) : delay_jobs.call
      else
        delay_jobs
      end
    end

    def initialize(options = {})
      @failed_reserve_count = 0

      [:quiet, :min_priority, :max_priority, :sleep_delay, :read_ahead, :queues, :exit_on_complete].each do |option|
        self.class.send("#{option}=", options[option]) if options.key?(option)
      end

      # Reset lifecycle on the offhand chance that something lazily
      # triggered its creation before all plugins had been registered.
      self.class.setup_lifecycle
    end

    def start # rubocop:disable CyclomaticComplexity, PerceivedComplexity
      puts 'Starting job worker'

      self.class.lifecycle.run_callbacks(:execute, self) do
        begin
          loop do
            if thread_pool.remaining_capacity > 0
              thread_pool.post do
                ExecuteJob.new(
                  default_max_attempts: self.class.max_attempts,
                  default_max_runtime: self.class.max_run_time,
                  default_log_level: self.class.default_log_level,
                  logger: self.class.logger,
                  quiet: self.class.quiet
                ).perform
              end
            else
              sleep self.class.sleep_delay
            end
          end
        rescue SignalException
          thread_pool.kill
          clear_orphaned_jobs!
          thread_pool.wait_for_termination
        end
      end
    end

    def thread_pool
      @thread_pool ||= Concurrent::FixedThreadPool.new(self.class.pool_size, max_queue: self.class.pool_size)
    end

    def clear_orphaned_jobs!
      Delayed::Job.where('locked_by like ?', "%pid:%#{Process.pid}%").update_all locked_at: nil, locked_by: nil
    end

    def work_off(count = 100)
      ExecuteJob.new(
        default_max_attempts: 25,
        default_max_runtime: 4.hours,
        default_log_level: 'info'.freeze,
        logger: self.class.logger,
        quiet: self.class.quiet,
        count: count
      ).perform
    end

    class ExecuteJob
      def initialize(default_max_attempts:, default_max_runtime:, default_log_level:, logger:, quiet:, count: 1)
        @default_max_attempts = default_max_attempts
        @default_max_runtime = default_max_runtime
        @default_log_level = default_log_level
        @logger = logger
        @quiet = quiet
        @count = count
      end

      def perform
        ActiveRecord::Base.connection_pool.with_connection do
          work_off count
        end
      end

      def name
        "host:#{Socket.gethostname} pid:#{Process.pid} thread:#{Thread.current.object_id}"
      rescue
        "pid:#{Process.pid} thread:#{Thread.current.object_id}"
      end

      private

      attr_reader :default_max_attempts, :default_max_runtime, :logger, :default_log_level, :quiet, :count

      def work_off(num = 100)
        success = 0
        failure = 0

        num.times do
          case reserve_and_run_one_job
          when true
            success += 1
          when false
            failure += 1
          else
            break
          end
        end

        [success, failure]
      end

      def run(job)
        job_say job, 'RUNNING'
        runtime = Benchmark.realtime do
          Timeout.timeout(max_run_time(job).to_i, WorkerTimeout) {
            job.invoke_job
          }
          job.destroy
        end
        job_say job, format('COMPLETED after %.4f', runtime)
        return true
      rescue DeserializationError => error
        job_say job, "FAILED permanently with #{error.class.name}: #{error.message}", 'error'

        job.error = error
        failed(job)
      rescue Exception => error # rubocop:disable RescueException
        logger.info error.backtrace
        handle_failed_job(job, error)
        return false
      end

      # Reschedule the job in the future (when a job fails).
      # Uses an exponential scale depending on the number of failed attempts.
      def reschedule(job, time = nil)
        if (job.attempts += 1) < max_attempts(job)
          time ||= job.reschedule_at
          job.run_at = time
          job.unlock
          job.save!
        else
          job_say job, "REMOVED permanently because of #{job.attempts} consecutive failures", 'error'
          failed(job)
        end
      end

      def failed(job)
        begin
          job.hook(:failure)
        rescue => error
          say "Error when running failure callback: #{error}", 'error'
          say error.backtrace.join("\n"), 'error'
        ensure
          job.destroy_failed_jobs? ? job.destroy : job.fail!
        end
      end

      def job_say(job, text, level = default_log_level)
        text = "Job #{job.name} (id=#{job.id})#{say_queue(job.queue)} #{text}"
        say text, level
      end

      def say(text, level = default_log_level)
        text = "[Worker(#{name})] #{text}"
        puts text unless quiet
        logger.send level, "#{Time.now.strftime('%FT%T%z')}: #{text}"
      end

      def max_attempts(job)
        job.max_attempts || default_max_attempts
      end

      def max_run_time(job)
        job.max_run_time || default_max_runtime
      end

      def say_queue(queue)
        " (queue=#{queue})" if queue
      end

      def handle_failed_job(job, error)
        job.error = error
        job_say job, "FAILED (#{job.attempts} prior attempts) with #{error.class.name}: #{error.message}", 'error'
        reschedule(job)
      end

      def reserve_and_run_one_job
        job = reserve_job
        run(job) if job
      end

      def reserve_job
        job = Delayed::Job.reserve(self)
        @failed_reserve_count = 0
        job
      rescue ::Exception => error # rubocop:disable RescueException
        say "Error while reserving job: #{error}"
        Delayed::Job.recover_from(error)
        @failed_reserve_count += 1
        raise FatalBackendError if @failed_reserve_count >= 10
        nil
      end
    end
  end
end

Delayed::Worker.reset
