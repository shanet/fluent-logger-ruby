# frozen_string_literal: true

module Fluent
  module Logger
    # Async logger that actually never blocks.
    #
    # Will drop logs if its own internal buffer size gets too big.
    class AsyncFluentLogger < FluentLogger
      MAX_CHUNKS = 1024

      def initialize(*args)
        super(*args)
        @use_nonblock = true
        @write_queue = SizedQueue.new(MAX_CHUNKS)
        @write_thread = start_write_thread(@write_queue)
      end

      def start_write_thread(queue)
        Thread.start do
          loop do
            data = @write_queue.pop
            written = 0
            while written <= data.bytesize
              remaining = data.bytesize - written

              len = @con.write_nonblock(
                data.byteslice(written, remaining),
                exception: false
              )
              if len == :wait_writable # (sp)
                IO.select(nil, [@con])
              else
                written += len
              end
            end
          end
        end
      end

      def send_data_nonblock(data)
        begin
          @write_queue.push(data, true)
        rescue ThreadError => e
          e
        end
      end
    end
  end
end
