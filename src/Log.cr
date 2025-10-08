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
          k = (Lawn.read @io).not_nil!
          v = Lawn.read @io
          yield({k, v})
        rescue IO::EOFError
          break
        end
      end
    end

    def write(batch : Hash(Bytes, Bytes?))
      buf = IO::Memory.new
      batch.each do |k, v|
        Lawn.write @io, k
        Lawn.write @io, v
      end
      @io.write buf.to_slice unless buf.empty?
    end

    def truncate
      @io.truncate
    end
  end
end
