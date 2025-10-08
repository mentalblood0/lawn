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

    getter header_size : UInt64 { @data_size_size + @pointer_size }

    @[YAML::Field(ignore: true)]
    getter headers : AlignedList { AlignedList.new headers_io, header_size }

    getter blocks_pointers_dir : String

    @[YAML::Field(ignore: true)]
    getter blocks_pointers_by_number : Hash(UInt8, AlignedList) = {} of UInt8 => AlignedList

    def blocks_pointers(n : UInt8)
      blocks_pointers[n] = AlignedList.new(
        io: (File.new (Path.new @blocks_pointers_dir) / "#{n.rjust 2, '0'}_blocks_pointers.bin"),
        element_size: n * @pointer_size) unless blocks_pointers.has_key? n
      blocks_pointers[n]
    end

    def initialize(@data_size_size, @pointer_size, @headers_io, @blocks_pointers_dir)
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
