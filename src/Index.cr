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
    getter cache : IO::Memory do
      file_size = file.size
      bytes = Bytes.new (file_size < @cache_size) ? file_size : @cache_size
      file.pos = 0
      file.read_fully bytes
      IO::Memory.new bytes
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
        cache.pos = pos
        read cache
      else
        file.pos = pos
        read file
      end
    end

    def each(from : Int64 = 0, &)
      pos = 1 + from * element_size
      if pos + element_size <= cache.size
        cache.pos = pos
        loop do
          yield read cache rescue break
          pos += element_size
        end
      end
      file.pos = pos
      loop { yield read file rescue break }
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
