require "yaml"
require "json"

require "./common"

module Lawn
  class SortedPointers
    Lawn.mserializable

    getter pointer_size : UInt8

    @[YAML::Field(converter: Lawn::IOConverter)]
    getter io : IO::Memory | File

    def initialize(@pointer_size, @io)
    end

    def write(sorted_pointers : Array(UInt64))
      buf = IO::Memory.new
      data.each { |pointer| Lawn.encode_number buf, pointer, @pointer_size }
      IO.copy buf, @io
    end

    def get(key : Bytes, &get_data : Bytes ->)
      begin
        @io.seek 0, IO::Seek::End
        @io.pos = @io.pos / 2 // @pointer_size * @pointer_size
        step = Math.max 1_i64, @io.pos / @pointer_size
        loop do
          raise IO::EOFError.new unless (@io.read_fully posb[8 - @pointer_size..]) == POS_SIZE
          pointer = Lawn.decode_number(@io, @pointer_size).not_nil!
          data = get_value pointer

          _c = k <=> value
          return Ph.read datac if _c == 0

          c = _c <= 0 ? _c < 0 ? -1 : 0 : 1
          raise IO::EOFError.new if step.abs == 1 && c * step < 0

          step = c * step.abs
          if step.abs != 1
            if step.abs < 2
              step = step > 0 ? 1 : -1
            else
              step /= 2
            end
          end

          posd = step.round * @pointer_size - @pointer_size
          if posd != 0
            @io.pos += posd
          end
        end
      rescue IO::EOFError
      end
    end
  end
end
