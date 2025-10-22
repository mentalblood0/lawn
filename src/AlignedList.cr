require "syscall"
require "json"
require "yaml"

require "./common"
require "./DataStorage"

module Lawn
  Syscall.def_syscall pwrite64, LibC::SSizeT, fd : Int32, buf : UInt8*, count : LibC::SizeT, offset : LibC::OffT

  class AlignedList < DataStorage(Int64)
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
      ::Log.debug { "AlignedList{#{path}}.initialize" }
      after_initialize
    end

    protected def init_head
      return unless (@head = get_head).all? { |b| b == 255 } rescue nil
      set_head Bytes.new head_size, 255
    end

    def after_initialize
      ::Log.debug { "AlignedList{#{path}}.after_initialize" }
      init_head
    end

    def clear
      ::Log.debug { "AlignedList{#{path}}.clear" }
      file.truncate
      after_initialize
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

    def get(i : Int64, size : Int32)
      ::Log.debug { "AlignedList{#{path}}.get #{i}" }
      result = Bytes.new Math.min @element_size, size
      read = Crystal::System::FileDescriptor.pread file, result, head_size + i * @element_size
      raise "pread returned #{read} although size of data to read is #{result.size}" unless read == result.size
      result
    end

    def get(id : Int64) : Bytes?
      get id, @element_size
    end

    protected def set(i : Int64, b : Bytes) : Int64
      ::Log.debug { "AlignedList{#{path}}.set #{i} #{b.hexstring}" }
      written = Lawn.pwrite64 file.fd, b.to_unsafe, b.size.to_u64, (head_size + i * @element_size).to_i64
      raise "pwrite64 returned #{written} although size of data to write is #{b.size}" unless written == b.size
      i
    end

    def get_head
      ::Log.debug { "AlignedList{#{path}}.get_head" }
      result = Bytes.new head_size
      read = Crystal::System::FileDescriptor.pread file, result, 0
      raise "pread returned #{read} although size of data to read is #{result.size}" unless read == result.size
      result
    end

    protected def set_head(b : Bytes)
      ::Log.debug { "AlignedList{#{path}}.set_head #{b.hexstring}" }
      @head = b
      written = Lawn.pwrite64 file.fd, b.to_unsafe, b.size.to_u64, 0_i64
      raise "pwrite64 returned #{written} although size of data to write is #{b.size}" unless written == b.size
    end

    def update(add : Array(Bytes), delete : Array(Int64)? = nil) : Array(Int64)
      ::Log.debug { "AlignedList{#{path}}.update add: #{add.map &.hexstring}, delete: #{delete}" }

      rs = [] of Int64

      replaced = 0
      if delete && !delete.empty?
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
          elements_count = (file.size - head_size) // @element_size
          (elements_count..elements_count + add.size - i - 1).each { |r| rs << r }
          set elements_count, Bytes.join add[i..].map { |d| @element_size > d.size ? d + Bytes.new(@element_size - d.size) : d }
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
