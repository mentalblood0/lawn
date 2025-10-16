require "log"

require "./exceptions"

module Lawn
  macro mserializable
    include YAML::Serializable
    include YAML::Serializable::Strict
    include JSON::Serializable
    include JSON::Serializable::Strict
  end

  macro mignore
    @[YAML::Field(ignore: true)]
    @[JSON::Field(ignore: true)]
  end

  alias Key = Bytes
  alias Value = Bytes
  alias KeyValue = {Key, Value}

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

  def self.decode_number(io : IO, size : UInt8) : UInt64
    raise Exception.new "Size must be from 1 to 8, not #{size}" unless 1 <= size <= 8
    rb = Bytes.new 8
    io.read_fully rb[0 - size..]
    case size
    when 1          then (IO::ByteFormat::BigEndian.decode UInt8, rb[-1..]).to_u64!
    when 2          then (IO::ByteFormat::BigEndian.decode UInt16, rb[-2..]).to_u64!
    when 3, 4       then (IO::ByteFormat::BigEndian.decode UInt32, rb[-4..]).to_u64!
    when 5, 6, 7, 8 then IO::ByteFormat::BigEndian.decode UInt64, rb
    else                 raise Exception.new "Unsupported number size #{size}"
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

  def self.decode_number_with_size(io : IO) : UInt64
    header = decode_number io, 1
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

  def self.encode_bytes_with_size(io : IO, b : Bytes, size_size : UInt8)
    encode_number io, b.size, size_size
    encode_bytes io, b
  end

  def self.decode_bytes_with_size(io : IO, size_size : UInt8) : Bytes?
    size = decode_number io, size_size
    decode_bytes io, size
  end

  def self.encode_bytes_with_size_size(io : IO, b : Bytes?)
    encode_number_with_size io, b.size
    encode_bytes io, b
  end

  def self.decode_bytes_with_size_size(io : IO) : Bytes?
    size = decode_number_with_size io
    decode_bytes io, size
  end
end
