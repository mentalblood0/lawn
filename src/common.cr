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

  module FileConverter
    alias Args = NamedTuple(
      filename: Path | String,
      mode: String,
      perm: File::Permissions,
      sync: Bool)

    def self.from_yaml(ctx : YAML::ParseContext, node : YAML::Nodes::Node) : File
      args = Args.new ctx, node
      Dir.mkdir_p (Path.new args[:filename]).parent
      r = File.new filename: args[:filename], mode: args[:mode], perm: args[:perm]
      r.sync = args[:sync]
      r
    end

    def self.to_yaml(value : IO | File, builder : YAML::Nodes::Builder)
      {filename: value.path, perm: value.info.permissions, sync: value.sync?}.to_yaml builder
    end
  end

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

  def self.encode_number(io : IO, n, size : UInt8)
    b = Bytes.new size
    size.times do |i|
      shift = (size - 1 - i) * 8
      b[i] = ((n >> shift) & 0xFF).to_u8!
    end
    io.write b
  end

  def self.encode_number(n, size : UInt8)
    io = IO::Memory.new size
    encode_number io, n, size
    io.to_slice
  end

  def self.decode_number(io : IO, size : UInt8) : UInt64?
    raise Exception.new "Size must be from 1 to 8, not #{size}" unless 1 <= size <= 8
    rb = Bytes.new 8
    io.read_fully rb[0 - size..]
    return nil if rb[0 - size..].all? { |b| b == 255_u8 }
    case size
    when 1          then (IO::ByteFormat::BigEndian.decode UInt8, rb[-1..]).to_u64!
    when 2          then (IO::ByteFormat::BigEndian.decode UInt16, rb[-2..]).to_u64!
    when 3, 4       then (IO::ByteFormat::BigEndian.decode UInt32, rb[-4..]).to_u64!
    when 5, 6, 7, 8 then IO::ByteFormat::BigEndian.decode UInt64, rb
    end
  end

  def self.encode_number_with_size(io : IO, n)
    if 0 <= n <= 255 - 8
      encode_number io, n, 1
    else
      size = (n.bit_length / 8).ceil.to_u64
      header = 255 - size
      encode_number io, header, 1
      encode_number io, n, size.to_u8
    end
  end

  def self.decode_number_with_size(io : IO) : UInt64?
    header = decode_number(io, 1).not_nil! rescue return nil
    return header if 0 <= header <= 255 - 8
    size = 255 - header
    decode_number io, size.to_u8
  end

  def self.encode_bytes(io : IO, b : Bytes)
    io.write b
  end

  def self.decode_bytes(io : IO, size : UInt64)
    r = Bytes.new size
    io.read_fully r
    r
  end

  def self.encode_bytes_with_size(io : IO, b : Bytes?, size_size : UInt8)
    case b
    when nil then encode_bytes io, Bytes.new size_size.to_i32, 255_u8
    else
      encode_number io, b.size, size_size
      encode_bytes io, b
    end
  end

  def self.decode_bytes_with_size(io : IO, size_size : UInt8) : Bytes?
    case size = decode_number io, size_size
    when nil then nil
    else          decode_bytes io, size
    end
  end

  def self.encode_bytes_with_size_size(io : IO, b : Bytes?)
    case b
    when nil then encode_number io, 255, 1
    else
      encode_number_with_size io, b.size
      encode_bytes io, b
    end
  end

  def self.decode_bytes_with_size_size(io : IO) : Bytes?
    case size = decode_number_with_size io
    when nil then nil
    else          decode_bytes io, size
    end
  end
end
