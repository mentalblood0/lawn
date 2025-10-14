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
      sorted_pointers.each { |pointer| Lawn.encode_number buf, pointer, @pointer_size }
      @io.write buf.to_slice
    end

    def get(key : Bytes, get_data : Proc(UInt64, Bytes), key_size_size : UInt8) : {header_pointer: UInt64, value: Bytes?}?
      begin
        @io.seek 0, IO::Seek::End
        @io.pos = @io.pos / 2 // @pointer_size * @pointer_size
        step = Math.max 1_i64, @io.pos / @pointer_size
        loop do
          header_pointer = Lawn.decode_number(@io, @pointer_size).not_nil!
          data = IO::Memory.new get_data.call header_pointer
          current_value = Lawn.decode_bytes_with_size data, key_size_size
          current_key = data.getb_to_end

          _c = key <=> current_key
          return {header_pointer: header_pointer, value: current_value} if _c == 0

          c = _c <= 0 ? _c < 0 ? -1 : 0 : 1
          return nil if step.abs == 1 && c * step < 0

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
