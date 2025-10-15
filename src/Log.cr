require "json"
require "yaml"

require "./common"

module Lawn
  class Log
    Lawn.mserializable

    @[YAML::Field(converter: Lawn::IOConverter)]
    getter io : IO::Memory | File

    def initialize(@io)
    end

    def read(&)
      @io.pos = 0
      loop do
        begin
          key = (Lawn.decode_bytes_with_size_size @io).not_nil! rescue break
          value = Lawn.decode_bytes_with_size_size @io
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
        Lawn.encode_bytes_with_size_size @io, k
        Lawn.encode_bytes_with_size_size @io, v
      end
      @io.write buf.to_slice
    end

    def truncate
      case c = @io
      when IO::Memory then c.clear
      when File       then c.truncate
      end
    end
  end
end
