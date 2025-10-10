require "json"
require "yaml"

require "./common.cr"
require "./exceptions"

module Lawn
  class AlignedList
    Lawn.mserializable

    class Exception < ::Lawn::Exception
    end

    getter element_size : UInt32

    @[YAML::Field(converter: Lawn::IOConverter)]
    getter io : IO::Memory | File

    def initialize(@io, @element_size)
      after_initialize
    end

    def after_initialize
      @io.pos = 0
      return unless read.all? { |b| b == 255 } rescue nil
      io.write Bytes.new @element_size.to_i32, 255
    end

    protected def read
      r = Bytes.new @element_size
      @io.read_fully r
      r
    end

    protected def as_p(b : Bytes)
      r = 0_u64
      b.each { |b| r = (r << 8) + b }
      r
    end

    protected def as_b(f : UInt64)
      r = Bytes.new Math.max 8, @element_size
      IO::ByteFormat::BigEndian.encode f, r[(Math.max 8, @element_size) - 8..]
      (@element_size >= 8) ? r : r[8 - @element_size..]
    end

    def get(i : UInt64)
      @io.pos = i * @element_size
      read
    end

    protected def set(i : UInt64, b : Bytes) : UInt64
      @io.pos = i * @element_size
      @io.write b
      i
    end

    def add(b : Bytes) : UInt64
      ::Log.debug { "AlignedList.add #{b.hexstring}" }
      raise Exception.new "Element size must be <= #{@element_size}, not #{b.size}" unless b.size <= @element_size

      b += Bytes.new @element_size - b.size if b.size < @element_size

      @io.pos = 0
      h = read
      if h.all? { |b| b == 255 }
        @io.seek 0, IO::Seek::End
        r = @io.pos.to_u64! // @element_size
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

    def delete(i : UInt64) : UInt64
      ::Log.debug { "AlignedListdelete #{i}" }

      if size > 2 * @element_size
        set i, get 0
        set 0, as_b i
      else
        case @io
        when File
          @io.as(File).truncate @element_size.to_i32
        when IO::Memory
          @io.as(IO::Memory).clear
          @io.write Bytes.new @element_size.to_i32, 255
        end
      end
      i
    end

    def replace(i : UInt64, b : Bytes) : UInt64
      ::Log.debug { "AlignedListreplace #{i} #{b.hexstring}" }
      raise Exception.new "Element size must be <= #{@element_size}, not #{b.size}" unless b.size <= @element_size

      b += Bytes.new @element_size - b.size if b.size < @element_size

      set i, b
    end
  end
end
