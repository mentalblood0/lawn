module Lawn
  module SizeConverter
    def self.from_yaml(ctx : YAML::ParseContext, node : YAML::Nodes::Node) : Int64
      case node
      when YAML::Nodes::Scalar
        case (size_string = node.value).downcase
        when /(\d+)$/, /(\d+) *b$/ then $1.to_i64
        when /(\d+) *kb$/          then $1.to_i64 * 1024
        when /(\d+) *mb$/          then $1.to_i64 * 1024 * 1024
        when /(\d+) *gb$/          then $1.to_i64 * 1024 * 1024 * 1024
        else                            raise "Invalid size format: #{size_string}"
        end
      else node.raise "Invalid size format. Expected format like '2MB', '1GB', etc., not #{node.kind}"
      end
    end

    def self.to_yaml(value : Int64, builder : YAML::Nodes::Builder)
      builder.scalar value.to_s
    end
  end

  abstract class Index(T)
    Lawn.mserializable

    getter path : Path

    @[YAML::Field(converter: Lawn::SizeConverter)]
    getter cache_size : Int64

    Lawn.mignore
    getter file : File do
      unless File.exists? @path
        Dir.mkdir_p @path.parent
        File.touch @path
      end
      File.new @path, "r"
    end

    Lawn.mignore
    getter cache : Bytes do
      result = Bytes.new Math.min bytesize, @cache_size
      Crystal::System::FileDescriptor.pread file, result, 0
      result
    end

    Lawn.mignore
    getter schema_byte : UInt8 do
      if cache.empty?
        result = 0_u8
        LibC.pread file.fd, pointerof(result), 1, 0
        result
      else
        cache[0]
      end
    end

    Lawn.mignore
    getter pointer_size : UInt8 do
      schema_byte // 2
    end

    Lawn.mignore
    getter? is_for_fixed_table : Bool do
      (schema_byte % 2) == 1
    end

    Lawn.mignore
    getter bytesize : Int64 { file.size }

    Lawn.mignore
    getter size : Int64 { (bytesize == 0) ? 0_i64 : (bytesize - 1) // element_size }

    abstract def element_size : UInt8
    abstract def read(source : IO = file) : T

    def clear
      file.delete
      @file = nil
      @pointer_size = nil
      @size = nil
      @bytesize = nil
    end

    def [](i : Int64) : T
      offset = 1 + i * element_size
      if offset + element_size <= cache.size
        read IO::Memory.new cache[offset, element_size]
      else
        buf = Bytes.new element_size
        Crystal::System::FileDescriptor.pread file, buf, offset
        read IO::Memory.new buf
      end
    end

    class Cursor(T)
      getter index : Index(T)
      getter i : Int64
      getter value : T?

      protected def update_value
        @value = (0 <= @i < @index.size) ? @index[@i] : nil
      end

      def initialize(@index, @i = 0_i64)
        update_value
      end

      def next : T?
        @i += 1
        update_value
        value
      end

      def previous : T?
        @i -= 1
        update_value
        value
      end
    end
  end
end
