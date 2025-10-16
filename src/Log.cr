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
      r = File.new @path, "a"
      r.sync = true
      r
    end

    def initialize(@path)
    end

    def read(&)
      file.pos = 0
      loop do
        begin
          key = (Lawn.decode_bytes_with_size_size file).not_nil! rescue break
          value = Lawn.decode_bytes_with_size_size file
          yield({key, value})
        rescue IO::EOFError
          break
        end
      end
    end

    def write(batch : Array(KeyValue))
      return if batch.empty?
      buf = IO::Memory.new
      batch.each do |key, value|
        Lawn.encode_bytes_with_size_size file, key
        Lawn.encode_bytes_with_size_size file, value
      end
      file.write buf.to_slice
    end

    def clear
      file.truncate
    end
  end
end
