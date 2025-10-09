require "json"
require "yaml"

require "./common"
require "./Codable"

module Lawn
  class Log
    Lawn.mserializable

    record Entry,
      key : Bytes,
      value : Bytes? { include Lawn::Codable }

    @[YAML::Field(converter: Lawn::IOConverter)]
    getter io : IO::Memory | File

    def read(&)
      @io.pos = 0
      loop do
        begin
          yield Entry.new @io
        rescue IO::EOFError
          break
        end
      end
    end

    def write(batch : Array(Log::Entry))
      buf = IO::Memory.new
      batch.each &.encode buf
      @io.write buf.to_slice unless buf.empty?
    end

    def truncate
      @io.truncate
    end
  end
end
