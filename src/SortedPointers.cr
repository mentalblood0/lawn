require "yaml"
require "json"

require "./common"
require "./RoundDataStorage"

module Lawn
  class SortedPointers
    Lawn.mserializable

    getter pointer_size : UInt8

    @[YAML::Field(converter: Lawn::IOConverter)]
    getter io : IO::Memory | File

    def initialize(@pointer_size, @io)
    end

    def write(sorted_pointers : Array(RoundDataStorage::Id))
      buf = IO::Memory.new
      sorted_pointers.each do |size_exponent, pointer|
        Lawn.encode_number buf, size_exponent, 1
        Lawn.encode_number buf, pointer, @pointer_size
      end
      @io.write buf.to_slice
    end

    def get(key : Bytes, get_data : Proc(RoundDataStorage::Id, Bytes), key_size_size : UInt8) : {data_id: RoundDataStorage::Id, value: Bytes?}?
      ::Log.debug { "SortedPointers.get #{key.hexstring}" }
      begin
        @io.seek 0, IO::Seek::End
        @io.pos = @io.pos / 2 // @pointer_size * @pointer_size
        step = Math.max 1_i64, @io.pos / @pointer_size
        loop do
          size_exponent = Lawn.decode_number(@io, 1).not_nil!.to_u8
          pointer = Lawn.decode_number(@io, @pointer_size).not_nil!
          data_id = {size_exponent, pointer}

          data = IO::Memory.new get_data.call data_id
          current_value = Lawn.decode_bytes_with_size data, key_size_size
          current_key = data.getb_to_end

          _c = key <=> current_key
          return {data_id: data_id, value: current_value} if _c == 0

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
