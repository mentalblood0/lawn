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

    Lawn.mignore
    getter headers : AlignedList { AlignedList.new headers_io, @data_size_size.to_u32 + @pointer_size }

    getter segments_pointers_dir : String

    Lawn.mignore
    getter segments_pointers_by_number : Array(AlignedList?) = Array(AlignedList?).new 32 { nil }

    getter segments_dir : String

    Lawn.mignore
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

    alias Id = UInt64

    class Segment
      getter value : Bytes
      getter size_exponent : UInt8
      property pointer : UInt64 = 0_u64

      def initialize(@value, @size_exponent)
      end
    end

    alias PointersEncoded = {add_index: Int32, value: Bytes}

    def update(add : Array(Bytes), delete : Array(Id)) : Array(Id)
      ::Log.debug { "SplitDataStorage.update add: #{add.map &.hexstring}, delete: #{delete}" }

      delete_segments_by_size_exponent = Array(Array(UInt64)?).new(32) { nil }
      delete_pointers_by_total = Array(Array(UInt64)?).new(32) { nil }
      delete.each do |header_pointer|
        header_encoded = IO::Memory.new (headers.get header_pointer).not_nil! rescue next
        data_size = (Lawn.decode_number header_encoded, @data_size_size).not_nil!
        pointers_pointer = (Lawn.decode_number header_encoded, @pointer_size).not_nil!

        sizes = split data_size
        pointers_encoded = IO::Memory.new ((segments_pointers sizes.size.to_u8).get pointers_pointer).not_nil!
        pointers = Array.new(sizes.size) { |i| (Lawn.decode_number pointers_encoded, @pointer_size).not_nil! }

        (0..pointers.size - 1).each do |p|
          size_exponent = sizes[p].bit_length - 1
          delete_segments_by_size_exponent[size_exponent] = Array(UInt64).new unless delete_segments_by_size_exponent[size_exponent]
          delete_segments_by_size_exponent[size_exponent].not_nil! << pointers[p]
        end
        total = sizes.size
        delete_pointers_by_total[total] = Array(UInt64).new unless delete_pointers_by_total[total]
        delete_pointers_by_total[total].not_nil! << pointers_pointer
      end

      segments_by_size_exponent = Array(Array(Segment)?).new(32) { nil }
      segments_by_add_index = Array(Array(Segment)).new(add.size) { Array(Segment).new }
      add.each_with_index do |d, add_index|
        sizes = split d.size
        i = 0
        sizes.each_with_index do |size, size_index|
          size_exponent = (size.bit_length - 1).to_u8
          segment = Segment.new(
            value: d[i..Math.min(i + size, d.size) - 1],
            size_exponent: size_exponent)
          segments_by_size_exponent[size_exponent] = Array(Segment).new unless segments_by_size_exponent[size_exponent]
          segments_by_size_exponent[size_exponent].not_nil! << segment
          segments_by_add_index[add_index] << segment
          i += size
        end
      end

      segments_by_size_exponent.each_with_index do |ss, se|
        case ss
        when nil then next
        else          segments(se.to_u8).update(add: ss.map &.value, delete: delete_segments_by_size_exponent[se]).each_with_index { |p, i| ss[i].pointer = p }
        end
      end

      pointers_encoded_by_total = Array(Array(PointersEncoded)?).new(32) { nil }
      segments_by_add_index.each_with_index do |ss, add_index|
        pointers_encoded_io = IO::Memory.new ss.size
        ss.each { |s| Lawn.encode_number pointers_encoded_io, s.pointer, @pointer_size }
        pointers_encoded_by_total[ss.size] = Array(PointersEncoded).new unless pointers_encoded_by_total[ss.size]
        pointers_encoded_by_total[ss.size].not_nil! << {add_index: add_index, value: pointers_encoded_io.to_slice}
      end
      pointers_pointer_by_add_index = Array(UInt64).new(add.size) { 0_u64 }
      pointers_encoded_by_total.each_with_index do |pse, total|
        case pse
        when nil then next
        else
          pointers_pointers = segments_pointers(total.to_u8).update add: pse.map(&.[:value]), delete: delete_pointers_by_total[total]
          (0..pse.size - 1).each { |i| pointers_pointer_by_add_index[pse[i][:add_index]] = pointers_pointers[i] }
        end
      end

      r = headers.update(
        add: (pointers_pointer_by_add_index.map_with_index do |pointers_pointer, add_index|
          header_encoded = IO::Memory.new @data_size_size + @pointer_size
          Lawn.encode_number header_encoded, add[add_index].size, @data_size_size
          Lawn.encode_number header_encoded, pointers_pointer, @pointer_size
          header_encoded.to_slice
        end),
        delete: delete)
    end

    def get(header_pointer : Id) : Bytes?
      ::Log.debug { "SplitDataStorage.get #{header_pointer}" }

      header_encoded = IO::Memory.new (headers.get header_pointer).not_nil! rescue return nil
      data_size = (Lawn.decode_number header_encoded, @data_size_size).not_nil!
      pointers_pointer = (Lawn.decode_number header_encoded, @pointer_size).not_nil!

      sizes = split data_size
      pointers_encoded = IO::Memory.new ((segments_pointers sizes.size.to_u8).get pointers_pointer).not_nil!
      pointers = Array.new(sizes.size) { |i| (Lawn.decode_number pointers_encoded, @pointer_size).not_nil! }

      segments = (0..pointers.size - 1).map do |p|
        ((segments (sizes[p].bit_length - 1).to_u8).get pointers[p]).not_nil!
      end
      data = (Slice.join segments)[..data_size - 1]
      ::Log.debug { "data = #{data}" }

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
