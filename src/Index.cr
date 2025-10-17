require "yaml"

require "./common"
require "./RoundDataStorage"

module Lawn
  class Index
    Lawn.mserializable

    getter path : Path
    getter read_chunk_size : Int64
    getter max_cache_size : Int64

    Lawn.mignore
    getter cache = {} of Int64 => RoundDataStorage::Id

    Lawn.mignore
    getter file : File do
      unless File.exists? @path
        Dir.mkdir_p @path.parent
        File.touch @path
      end
      File.new @path, "r"
    end

    getter pointer_size : UInt8

    Lawn.mignore
    getter size : Int64 = 0_i64

    getter id_size : UInt8 { @pointer_size + 1 }

    def initialize(@path, @pointer_size, @read_chunk_size, @max_cache_size)
      after_initialize
    end

    def after_initialize
      @size = file.size // id_size
    end

    def clear
      file.delete
      @file = nil
      after_initialize
    end

    protected def read(source : IO = file)
      rounded_size_index = file.read_byte.not_nil!
      pointer = Lawn.decode_number(file, @pointer_size).not_nil!
      {rounded_size_index: rounded_size_index, pointer: pointer}
    end

    protected def read(&)
      n = @read_chunk_size // id_size
      buf = IO::Memory.new n * id_size
      IO.copy file, buf, buf.size
      buf.rewind
      (0..n - 1).each { |shift| yield({read(buf), shift}) rescue return }
    end

    def [](i : Int64) : RoundDataStorage::Id
      unless cache.has_key? i
        cache.clear if (cache.size + @read_chunk_size // id_size) * (8 + 1 + 8) >= @max_cache_size
        file.pos = i * id_size
        read { |id, shift| cache[i + shift] = id }
      end
      cache[i]
    end

    def each(&)
      file.pos = 0
      @size.times { yield read }
    end

    def each_with_index(&)
      i = 0
      each do |id|
        yield({id, i})
        i += 1
      end
    end
  end
end
