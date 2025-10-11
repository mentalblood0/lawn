require "yaml"
require "json"

require "./common"
require "./AlignedList"

module Lawn
  class SplitDataStorage
    Lawn.mserializable

    getter data_size_size : UInt8
    getter pointer_size : UInt8

    @[YAML::Field(converter: Lawn::IOConverter)]
    getter headers_io : IO::Memory | File

    @[YAML::Field(ignore: true)]
    getter headers : AlignedList { AlignedList.new headers_io, @data_size_size.to_u32 + @pointer_size }

    getter segments_pointers_dir : String

    @[YAML::Field(ignore: true)]
    getter segments_pointers_by_number : Array(AlignedList?) = Array(AlignedList?).new 32 { nil }

    getter segments_dir : String

    @[YAML::Field(ignore: true)]
    getter segments_by_size_exponent : Array(AlignedList?) = Array(AlignedList?).new 32 { nil }

    def segments_pointers(n : UInt8)
      unless segments_pointers_by_number[n - 1]
        io = File.new (Path.new @segments_pointers_dir) / "segments_pointers_groups_of_#{n.to_s.rjust 2, '0'}.dat", "w+"
        io.sync = true
        @segments_pointers_by_number[n - 1] = AlignedList.new io, n * @pointer_size
      end
      segments_pointers_by_number[n - 1].not_nil!
    end

    def segments(size_exponent : UInt8)
      unless segments_by_size_exponent[size_exponent]
        io = File.new (Path.new @segments_dir) / "segments_of_size_#{(1 << size_exponent).to_s.rjust 10, '0'}byte.dat", "w+"
        io.sync = true
        segments_by_size_exponent[size_exponent] = AlignedList.new io, (1_u32 << size_exponent)
      end
      segments_by_size_exponent[size_exponent].not_nil!
    end

    def initialize(@data_size_size, @pointer_size, @headers_io, @segments_pointers_dir, @segments_dir)
    end

    def add(data : Bytes) : UInt64
      sizes = split data.size

      i = 0
      pointers = sizes.map do |size|
        p = (segments (size.bit_length - 1).to_u8).add data[i..(Math.min i + size, data.size) - 1]
        i += size
        p
      end

      pointers_encoded = IO::Memory.new pointers.size * @pointer_size
      pointers.each { |p| Lawn.encode_number pointers_encoded, p, @pointer_size }
      pointers_pointer = (segments_pointers pointers.size.to_u8).add pointers_encoded.to_slice

      header_encoded = IO::Memory.new @data_size_size + @pointer_size
      Lawn.encode_number header_encoded, data.size, @data_size_size
      Lawn.encode_number header_encoded, pointers_pointer, @pointer_size
      header_pointer = headers.add header_encoded.to_slice

      header_pointer
    end

    def get(header_pointer : UInt64) : Bytes?
      header_encoded = IO::Memory.new (headers.get header_pointer).not_nil! rescue return nil
      data_size = (Lawn.decode_number header_encoded, @data_size_size).not_nil!
      pointers_pointer = (Lawn.decode_number header_encoded, @pointer_size).not_nil!

      sizes = split data_size
      pointers_encoded = IO::Memory.new ((segments_pointers sizes.size.to_u8).get pointers_pointer).not_nil!
      pointers = Array.new(sizes.size) { |i| (Lawn.decode_number pointers_encoded, @pointer_size).not_nil! }

      segments = (0..pointers.size - 1).map { |p| ((segments (sizes[p].bit_length - 1).to_u8).get pointers[p]).not_nil! }
      data = segments.sum[..data_size - 1]

      data
    end

    def delete(header_pointer : UInt64) : UInt64
      header_encoded = IO::Memory.new (headers.get header_pointer).not_nil! rescue return header_pointer
      data_size = (Lawn.decode_number header_encoded, @data_size_size).not_nil!
      pointers_pointer = (Lawn.decode_number header_encoded, @pointer_size).not_nil!

      sizes = split data_size
      pointers_encoded = IO::Memory.new ((segments_pointers sizes.size.to_u8).get pointers_pointer).not_nil!
      pointers = Array.new(sizes.size) { |i| (Lawn.decode_number pointers_encoded, @pointer_size).not_nil! }

      (0..pointers.size - 1).map { |p| ((segments (sizes[p].bit_length - 1).to_u8).delete pointers[p]).not_nil! }
      (segments_pointers sizes.size.to_u8).delete pointers_pointer
      headers.delete header_pointer

      header_pointer
    end

    def split(n)
      fbi = n.trailing_zeros_count
      a = [(n.class.new 1) << fbi]
      asum = a[0]
      (fbi + 1..n.bit_length).each do |i|
        next unless (n.bit i) == 1
        b = (n.class.new 1) << i
        if ((a.size > 1) ? a.size * @pointer_size : 0) >= b - asum
          a = [b * 2]
          asum = b * 2
        else
          a << b
          asum += b
        end
      end
      a
    end
  end
end
