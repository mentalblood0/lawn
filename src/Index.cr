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
        File.write @path, Bytes.new 1, 1_u8
      end
      File.new @path, "r"
    end

    Lawn.mignore
    getter cache : Bytes do
      file_size = file.size
      result = Bytes.new (file_size < @cache_size) ? file_size : @cache_size
      Crystal::System::FileDescriptor.pread file, result, 0
      result
    end

    Lawn.mignore
    getter pointer_size : UInt8 do
      file.pos = 0
      file.read_byte.not_nil!
    end

    Lawn.mignore
    getter size : Int64 { (file.size - 1) // element_size }

    abstract def element_size : UInt8
    abstract def read(source : IO = file) : T

    def clear
      file.delete
      @file = nil
      @pointer_size = nil
      @size = nil
    end

    def [](i : Int64) : T
      pos = 1 + i * element_size
      if pos + element_size <= cache.size
        read IO::Memory.new cache[pos, element_size]
      else
        buf = Bytes.new element_size
        Crystal::System::FileDescriptor.pread file, buf, pos
        read IO::Memory.new buf
      end
    end

    def each(from : Int64 = 0, &)
      (from..size - 1).each { |i| yield self[i] }
    end

    def each_with_index(from : Int64 = 0, &)
      i = 0
      each(from) do |id|
        yield({id, i})
        i += 1
      end
    end
  end
end
