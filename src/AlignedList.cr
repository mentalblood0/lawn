require "json"
require "yaml"

require "./common.cr"

module Lawn
  class AlignedList
    Lawn.mserializable

    getter element_size : UInt32
    getter path : Path

    Lawn.mignore
    getter file : File do
      unless File.exists? @path
        Dir.mkdir_p @path.parent
        File.touch @path
      end
      r = File.new @path, "w+"
      r.sync = true
      r
    end

    Lawn.mignore
    getter head : Bytes = Bytes.new 0

    def initialize(@path, @element_size)
      after_initialize
    end

    protected def write_head
      file.pos = 0
      return unless (@head = read).all? { |b| b == 255 } rescue nil
      @head = Bytes.new @element_size.to_i32, 255
      file.write head
    end

    def after_initialize
      write_head

      file.seek 0, IO::Seek::End
    end

    protected def read
      r = Bytes.new @element_size
      file.read_fully r
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
      ::Log.debug { "AlignedList.get #{i}" }
      file.pos = i * @element_size
      read
    end

    protected def set(i : UInt64, b : Bytes) : UInt64
      @head = b if i == 0
      file.pos = i * @element_size
      file.write b
      i
    end

    def update(add : Array(Bytes), delete : Array(UInt64)? = nil) : Array(UInt64)
      ::Log.debug { "AlignedList.update add: #{add.map &.hexstring}, delete: #{delete}" }

      rs = [] of UInt64

      replaced = 0
      if delete
        while (replaced < add.size) && (replaced < delete.size)
          rs << set delete[replaced], add[replaced]
          replaced += 1
        end
        if replaced < delete.size
          set delete[replaced], @head
          (replaced + 1..delete.size - 1).each do |i|
            set delete[i], as_b delete[i - 1]
          end
          set 0, as_b delete.last
        end
      end

      (replaced..add.size - 1).each do |i|
        if @head.all? { |b| b == 255 }
          file.seek 0, IO::Seek::End
          rn = file.pos.to_u64 // @element_size
          (rn..rn + add.size - i - 1).each { |r| rs << r }
          file.write Bytes.join add[i..].map { |d| @element_size > d.size ? d + Bytes.new(@element_size - d.size) : d }
          break
        else
          r = as_p @head
          n1 = get r

          rs << set r, add[i]
          set 0, n1
        end
      end

      ::Log.debug { "AlignedList.update => #{rs}" }
      rs
    end
  end
end
