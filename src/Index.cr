require "yaml"

require "./common"
require "./RoundDataStorage"

module Lawn
  class Index
    Lawn.mserializable

    getter path : Path

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

    def initialize(@path, @pointer_size)
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

    def [](i : Int64) : RoundDataStorage::Id
      file.pos = i * id_size
      read
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
