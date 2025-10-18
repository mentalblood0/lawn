require "json"
require "yaml"

require "./common.cr"

module Lawn
  class AlignedList
    Lawn.mserializable

    getter element_size : Int32
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

    getter head_size : Int32 { Math.min(@element_size, 8) }

    def initialize(@path, @element_size)
      after_initialize
    end

    protected def init_head
      file.pos = 0
      return unless (@head = read).all? { |b| b == 255 } rescue nil
      @head = Bytes.new head_size, 255
      file.write head
    end

    def after_initialize
      init_head
    end

    def clear
      file.truncate
      after_initialize
    end

    protected def read(max_size : Int32 = @element_size)
      r = Bytes.new Math.min @element_size, max_size
      file.read_fully r
      r
    end

    protected def as_p(b : Bytes)
      r = 0_i64
      b[..Math.min 7, b.size - 1].each { |b| r = (r << 8) + b }
      r
    end

    protected def as_b(i : Int64)
      r = Bytes.new 8
      IO::ByteFormat::BigEndian.encode i, r
      (@element_size >= 8) ? r : r[8 - @element_size..]
    end

    def go_to(i : Int64)
      file.pos = head_size + i * @element_size
    end

    def get(i : Int64, size : Int32 = @element_size)
      ::Log.debug { "AlignedList{#{path}}.get #{i}" }
      go_to i
      read size
    end

    protected def set(i : Int64, b : Bytes) : Int64
      go_to i
      file.write b
      i
    end

    protected def set_head(b : Bytes)
      @head = b
      file.pos = 0
      file.write b
    end

    def update(add : Array(Bytes), delete : Array(Int64)? = nil) : Array(Int64)
      ::Log.debug { "AlignedList{#{path}}.update add: #{add.map &.hexstring}, delete: #{delete}" }

      rs = [] of Int64

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
          set_head as_b delete.last
        end
      end

      (replaced..add.size - 1).each do |i|
        if @head.all? { |b| b == 255 }
          file.seek 0, IO::Seek::End
          elements_count = (file.pos - head_size) // @element_size
          (elements_count..elements_count + add.size - i - 1).each { |r| rs << r }
          file.write Bytes.join add[i..].map { |d| @element_size > d.size ? d + Bytes.new(@element_size - d.size) : d }
          break
        else
          r = as_p @head
          n1 = get r, head_size

          rs << set r, add[i]
          set_head n1
        end
      end

      ::Log.debug { "AlignedList{#{path}}.update => #{rs}" }
      rs
    end
  end
end
