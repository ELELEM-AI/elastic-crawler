#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

# frozen_string_literal: true

require_dependency File.join(__dir__, 'base')

module Crawler
  module Logging
    module Handler
      attr_reader :event_logger, :logger_instance

      class FileHandler < Handler::Base
        def initialize(log_level, filename, rotation_period)
          super
          raise ArgumentError, 'Need a filename for FileHandler log handler!' unless filename

          # logger instance setup
          logger_instance = Logger.new(filename, rotation_period)
          logger_instance.level = log_level
          # Set a simple format to output only the message
          format_logger(logger_instance)
          @logger_instance = logger_instance
        end

        def log(message, message_log_level)
          case message_log_level
          when Logger::DEBUG
            @logger_instance.debug(message)
          when Logger::INFO
            @logger_instance.info(message)
          when Logger::WARN
            @logger_instance.warn(message)
          when Logger::ERROR
            @logger_instance.error(message)
          when Logger::FATAL
            @logger_instance.fatal(message)
          else
            @logger_instance << message
          end
        end

        def add_tags(*_tags)
          # Tags are ignored in the simplified log format
          # This method is kept for compatibility with existing code
        end

        def format_logger(logger_instance)
          logger_instance.formatter = proc do |_severity, _datetime, _progname, msg|
            "#{msg}\n"
          end
        end

        def level(log_level)
          @logger_instance.level = log_level
        end
      end
    end
  end
end
