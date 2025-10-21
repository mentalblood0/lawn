require "./common"

module Lawn
  class Cache
    Lawn.mserializable

    getter path : Path
    getter element_size : UInt8
    getter max_size : Int64

    Lawn.mignore
    getter data : IO::Memory { IO::Memory.new File.open(@path, "rb") { |file| file.getb_to_end } }

    def initialize(@path, @element_size, @max_size)
    end

    def clear
      @data = nil
    end

    def pos=(i : Int64)
      data.pos = 1 + i * @element_size
    end
  end
end
