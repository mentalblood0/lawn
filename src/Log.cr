require "json"
require "yaml"

require "./common"

module Lawn
  class Log
    Lawn.mserializable

    getter data_size_size : UInt8

    @[YAML::Field(converter: Lawn::IOConverter)]
    getter io : IO::Memory | File

    def initialize(@io, @data_size_size)
    end

    def read(&)
      @io.pos = 0
      loop do
        begin
          key = (Lawn.read_bytes_with_size @io, @data_size_size).not_nil!
          value = Lawn.read_bytes_with_size @io, @data_size_size
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
        Lawn.write_bytes_with_size @io, k, @data_size_size
        Lawn.write_bytes_with_size @io, v, @data_size_size
      end
      @io.write buf.to_slice
    end

    def truncate
      @io.truncate
    end
  end
end
