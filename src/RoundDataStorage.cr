require "yaml"
require "json"

require "./common"
require "./AlignedList"

module Lawn
  class RoundDataStorage
    Lawn.mserializable

    getter data_size_size : UInt8
    getter pointer_size : UInt8
    getter data_dir : String
    getter logarithmically_divided_sizes_scale : {max: Int32, points: Int32}

    @[YAML::Field(ignore: true)]
    getter data_aligned_lists_by_rounded_size_index : Array(AlignedList?) = Array(AlignedList?).new 256 { nil }

    def data_aligned_list(round_index : UInt8)
      unless data_aligned_lists_by_rounded_size_index[round_index]
        size = sizes[round_index]
        io = File.new (Path.new @data_dir) / "size_and_data_of_rounded_size_#{size.to_s.rjust 10, '0'}.dat", "w+"
        io.sync = true
        data_aligned_lists_by_rounded_size_index[round_index] = AlignedList.new io, size.to_u32
      end
      data_aligned_lists_by_rounded_size_index[round_index].not_nil!
    end

    getter sizes : Array(Int32) {
      p = @logarithmically_divided_sizes_scale
      rs = [] of Int32

      (0..p[:points] - 1).each do |i|
        log = Math.log(1) + (Math.log(p[:max]) - Math.log(1)) * i / (p[:points] - 1)
        r = Math.exp(log).round.to_i32
        rs << r
      end

      rs.uniq!
      rs
    }

    def initialize(@data_size_size, @pointer_size, @data_dir, @logarithmically_divided_sizes_scale)
    end

    alias Id = {UInt8, UInt64}

    alias Add = {data: Bytes, data_index: Int32}

    def update(add : Array(Bytes), delete : Array(Id)) : Array(Id)
      ::Log.debug { "RoundDataStorage.update add: #{add.map &.hexstring}, delete: #{delete}" }

      add_data_by_rounded_size_index = Array(Array(Add)?).new(sizes.size) { nil }
      add.each_with_index do |data, data_index|
        size_and_data_encoded = IO::Memory.new
        Lawn.encode_bytes_with_size size_and_data_encoded, data, @data_size_size
        i = round_index size_and_data_encoded.size
        add_data_by_rounded_size_index[i] = Array(Add).new unless add_data_by_rounded_size_index[i]
        add_data_by_rounded_size_index[i].not_nil! << {data: size_and_data_encoded.to_slice, data_index: data_index}
      end

      delete_pointers_by_rounded_size_index = Array(Array(UInt64)?).new(sizes.size) { nil }
      delete.each do |i, pointer|
        delete_pointers_by_rounded_size_index[i] = Array(UInt64).new unless delete_pointers_by_rounded_size_index[i]
        delete_pointers_by_rounded_size_index[i].not_nil! << pointer
      end

      r = Array(Id).new(add.size) { {0_u8, 0_u64} }
      (0_u8..sizes.size - 1).each do |i|
        i_add = add_data_by_rounded_size_index[i]
        i_delete = delete_pointers_by_rounded_size_index[i]
        if i_add || i_delete
          data_aligned_list(i)
            .update(add: (i_add ? i_add.map &.[:data] : [] of Bytes), delete: i_delete)
            .each_with_index { |pointer, add_index| r[i_add.not_nil![add_index][:data_index]] = {i, pointer} }
        end
      end

      ::Log.debug { "RoundDataStorage.update => #{r}" }
      r
    end

    def get(id : Id)
      ::Log.debug { "RoundDataStorage.get #{id}" }

      al = @data_aligned_lists_by_rounded_size_index[id[0]]
      return unless al

      size_and_data_encoded = IO::Memory.new al.get id[1]
      Lawn.decode_bytes_with_size size_and_data_encoded, @data_size_size
    end

    def round_index(size : Int32) : Int32
      sizes.bsearch_index { |n| n >= size }.not_nil!
    end
  end
end
