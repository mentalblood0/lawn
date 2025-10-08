require "json"
require "yaml"

require "./common.cr"

module Lawn
  class AlignedList
    Lawn.mserializable

    @[YAML::Field(converter: Lawn::IOConverter)]
    getter io : IO::Memory | File
    getter s : UInt8

    def initialize(@io, @s)
      after_initialize
    end

    def after_initialize
      @io.pos = 0
      return unless read.all? { |b| b == 255 } rescue nil
      io.write Bytes.new @s.to_i32!, 255
    end

    protected def read
      r = Bytes.new @s
      @io.read_fully r
      r
    end

    protected def as_p(b : Bytes)
      r = 0_u64
      b.each { |b| r = (r << 8) + b }
      r
    end

    protected def as_b(f : UInt64)
      r = Bytes.new Math.max 8, @s
      IO::ByteFormat::BigEndian.encode f, r[(Math.max 8, @s) - 8..]
      (@s >= 8) ? r : r[8 - @s..]
    end

    def get(i : UInt64)
      @io.pos = i * @s
      read
    end

    protected def set(i : UInt64, b : Bytes)
      @io.pos = i * @s
      @io.write b
    end

    def add(b : Bytes)
      ::Log.debug { "AlignedList.add #{b.hexstring}" }

      @io.pos = 0
      h = read
      if h.all? { |b| b == 255 }
        @io.seek 0, IO::Seek::End
        r = @io.pos.to_u64! // @s
        @io.write b

        r
      else
        r = as_p h
        n1 = get r

        set r, b
        set 0, n1

        r
      end
    end

    protected def size
      (@io.is_a? File) ? @io.as(File).size : @io.as(IO::Memory).size
    end

    def delete(i : UInt64)
      ::Log.debug { "AlignedListdelete #{i}" }

      if size > 2 * @s
        set i, get 0
        set 0, as_b i
      else
        case @io
        when File
          @io.as(File).truncate @s.to_i32!
        when IO::Memory
          @io.as(IO::Memory).clear
          @io.write Bytes.new @s.to_i32!, 255
        end
      end
    end

    def replace(i : UInt64, b : Bytes)
      ::Log.debug { "AlignedListreplace #{i} #{b.hexstring}" }

      set i, b
    end
  end
end
