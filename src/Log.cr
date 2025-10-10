require "json"
require "yaml"

require "./common"

module Lawn
  class Log
    Lawn.mserializable

    @[YAML::Field(converter: Lawn::IOConverter)]
    getter io : IO::Memory | File

    def read(&)
      @io.pos = 0
      loop do
        begin
          key = (Lawn.read_bytes_with_size @io, 2_u8).not_nil!
          value = Lawn.read_bytes_with_size @io, 2_u8
          yield({key, value})
        rescue IO::EOFError
          break
        end
      end
    end

    def write(batch : Hash(K, V))
      return if batch.empty?
      buf = IO::Memory.new
      batch.each do |k, v|
        Lawn.write_bytes_with_size @io, k, 2_u8
        Lawn.write_bytes_with_size @io, v, 2_u8
      end
      @io.write buf.to_slice
    end

    def truncate
      @io.truncate
    end
  end
end
