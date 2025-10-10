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

    getter header_size : UInt32 { @data_size_size.to_u32 + @pointer_size }

    @[YAML::Field(ignore: true)]
    getter headers : AlignedList { AlignedList.new headers_io, header_size }

    getter segments_pointers_dir : String

    @[YAML::Field(ignore: true)]
    getter segments_pointers_by_number : Hash(UInt8, AlignedList) = {} of UInt8 => AlignedList

    getter segments_dir : String

    @[YAML::Field(ignore: true)]
    getter segments_by_size : Hash(UInt32, AlignedList) = {} of UInt32 => AlignedList

    def segments_pointers(n : UInt8)
      @segments_pointers_by_number[n] = AlignedList.new(
        io: (File.new (Path.new @segments_pointers_dir) / "#{n.to_s.rjust 2, '0'}_segments_pointers.bin", "w+"),
        element_size: n * @pointer_size) unless segments_pointers_by_number.has_key? n
      segments_pointers_by_number[n]
    end

    def segments(size : UInt32)
      segments_by_size[size] = AlignedList.new(
        io: (File.new (Path.new @segments_dir) / "#{size.to_s.rjust 2, '0'}byte_segments.bin", "w+"),
        element_size: size) unless segments_by_size.has_key? size
      segments_by_size[size]
    end

    def initialize(@data_size_size, @pointer_size, @headers_io, @segments_pointers_dir, @segments_dir)
    end

    def add(data : Bytes) : UInt64
      sizes = split data.size

      i = 0
      pointers = sizes.map do |size|
        p = (segments size.to_u32).add data[i..(Math.min i + size, data.size) - 1]
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

      segments = (0..pointers.size - 1).map { |p| ((segments sizes[p].to_u32).get pointers[p]).not_nil! }
      data = segments.sum[..data_size - 1]

      data
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
