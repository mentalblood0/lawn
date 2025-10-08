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

    getter header_size : UInt32 { @data_size_size + @pointer_size }

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
        io: (File.new (Path.new @segments_pointers_dir) / "#{n.to_s.rjust 2, '0'}_segments_pointers.bin"),
        element_size: n * @pointer_size) unless segments_pointers_by_number.has_key? n
      segments_pointers_by_number[n]
    end

    def segments(size : UInt32)
      segments_by_size[n] = AlignedList.new(
        io: (File.new (Path.new @segments_dir) / "#{size.to_s.rjust 2, '0'}byte_segments.bin"),
        element_size: size) unless segments_by_size.has_key? size
      segments_by_size[n]
    end

    def initialize(@data_size_size, @pointer_size, @headers_io, @segments_pointers_dir, @segments_dir)
    end

    def fast_split(n)
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
