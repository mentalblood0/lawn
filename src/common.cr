require "log"

require "./exceptions"

module Lawn
  macro mserializable
    include YAML::Serializable
    include YAML::Serializable::Strict
    include JSON::Serializable
    include JSON::Serializable::Strict
  end

  alias K = Bytes
  alias V = Bytes?
  alias KV = {K, V}

  module IOConverter
    alias Args = NamedTuple(
      file: NamedTuple(
        filename: Path | String,
        mode: String,
        perm: File::Permissions),
      sync: Bool)

    def self.from_yaml(ctx : YAML::ParseContext, node : YAML::Nodes::Node) : IO::Memory | File
      begin
        args = Args.new ctx, node
        Dir.mkdir_p (Path.new args[:file][:filename]).parent
        r = File.new **args[:file]
        r.sync = args[:sync]
        r
      rescue YAML::ParseException
        node.raise "Expected #{Args} or String of value \"memory\", not #{node.kind}" unless ((String.new ctx, node) == "memory" rescue false)
        IO::Memory.new
      end
    end

    def self.to_yaml(value : IO | File, builder : YAML::Nodes::Builder)
      case value
      when IO::Memory then "memory".to_yaml builder
      when File       then {file: {filename: value.path, perm: value.info.permissions}, sync: value.sync?}.to_yaml builder
      else
        raise "Unsupported IO type: #{value.class}"
      end
    end
  end

  def self.read_size(io : IO, size_size : UInt8) : UInt64?
    rb = Bytes.new 8
    io.read_fully rb[0 - size_size..]
    return nil if rb[0 - size_size..].all? { |b| b == 255_u8 }
    case size_size
    when 1          then (IO::ByteFormat::BigEndian.decode UInt8, rb[-1..]).to_u64!
    when 2          then (IO::ByteFormat::BigEndian.decode UInt16, rb[-2..]).to_u64!
    when 3, 4       then (IO::ByteFormat::BigEndian.decode UInt32, rb[-4..]).to_u64!
    when 5, 6, 7, 8 then IO::ByteFormat::BigEndian.decode UInt64, rb
    else
      raise Exception.new "Size size #{size_size} is too big"
    end
  end

  def self.write_size(io : IO, size, size_size : UInt8)
    b = Bytes.new size_size
    size_size.times do |i|
      shift = (size_size - 1 - i) * 8
      b[i] = ((size >> shift) & 0xFF).to_u8!
    end
    io.write b
  end

  def self.read_bytes(io : IO, size : UInt64)
    r = Bytes.new size
    io.read_fully r
    r
  end

  def self.write_bytes(io : IO, b : Bytes)
    io.write b
  end

  def self.read_bytes_with_size(io : IO, size_size : UInt8) : Bytes?
    case size = read_size io, size_size
    when nil then nil
    else          read_bytes io, size
    end
  end

  def self.write_bytes_with_size(io : IO, b : Bytes?, size_size : UInt8)
    case b
    when nil then write_bytes io, Bytes.new size_size.to_i32, 255_u8
    else
      write_size io, b.size, size_size
      write_bytes io, b
    end
  end
end
