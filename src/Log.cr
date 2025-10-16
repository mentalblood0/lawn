require "json"
require "yaml"

require "./common"

module Lawn
  class Log
    Lawn.mserializable

    getter path : Path

    Lawn.mignore
    getter file : File do
      unless File.exists? @path
        Dir.mkdir_p @path.parent
        File.touch @path
      end
      r = File.new @path, "a+"
      r.sync = true
      r
    end

    def initialize(@path)
    end

    def clear
      file.truncate
    end

    def write(batch : Array({Key, Value?}))
      return if batch.empty?
      buf = IO::Memory.new
      batch.each do |key, value|
        Lawn.encode_bytes_with_size_size buf, key
        if value
          buf.write_byte 1_u8
          Lawn.encode_bytes_with_size_size file, value
        else
          buf.write_byte 0_u8
        end
      end
      file.write buf.to_slice
    end

    def read(&)
      file.pos = 0
      loop do
        begin
          key = Lawn.decode_bytes_with_size_size file
          value = case file.read_byte
                  when 1_u8 then Lawn.decode_bytes_with_size_size file
                  when 0_u8 then nil
                  end
          yield({key, value})
        rescue IO::EOFError
          break
        end
      end
    end
  end
end
