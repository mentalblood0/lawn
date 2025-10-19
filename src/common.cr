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
    r = Bytes.new 8
    IO::ByteFormat::BigEndian.encode n.to_i64, r
    io.write r[8 - size..]
  end

  def self.decode_number(io : IO, size : UInt8) : Int64
    rb = Bytes.new 8
    io.read_fully rb[0 - size..]
    IO::ByteFormat::BigEndian.decode Int64, rb
  end

  def self.number_size(n)
    (n.bit_length + 7).to_u8 >> 3
  end

  def self.encode_number_with_size(io : IO, n)
    if n <= 255 - 8
      IO::ByteFormat::BigEndian.encode n.to_u8, io
    else
      size = number_size n
      header = 255_u8 - size
      IO::ByteFormat::BigEndian.encode header, io
      encode_number io, n, size
    end
  end

  def self.decode_number_with_size(io : IO) : Int64
    header = IO::ByteFormat::BigEndian.decode UInt8, io
    return header.to_i64 if header <= 255 - 8
    size = 255_u8 - header
    decode_number io, size
  end

  def self.decode_bytes(io : IO, size : Int64)
    r = Bytes.new size
    io.read_fully r
    r
  end

  def self.encode_bytes_with_size(io : IO, b : Bytes, size_size : UInt8)
    encode_number io, b.size, size_size
    io.write b
  end

  def self.decode_bytes_with_size(io : IO, size_size : UInt8) : Bytes?
    size = decode_number io, size_size
    decode_bytes io, size
  end

  def self.encode_bytes_with_size_size(io : IO, b : Bytes?)
    encode_number_with_size io, b.size
    io.write b
  end

  def self.decode_bytes_with_size_size(io : IO) : Bytes?
    size = decode_number_with_size io
    decode_bytes io, size
  end
end
