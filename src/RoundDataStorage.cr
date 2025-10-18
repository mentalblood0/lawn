require "yaml"
require "json"

require "./common"
require "./AlignedList"

module Lawn
  class RoundDataStorage
    Lawn.mserializable

    getter dir : Path
    getter logarithmically_divided_sizes_scale : {max: Int32, points: Int32}

    Lawn.mignore
    getter data_aligned_lists_by_rounded_size_index : Array(AlignedList?) = Array(AlignedList?).new 256 { nil }

    def data_aligned_list(round_index : UInt8)
      unless data_aligned_lists_by_rounded_size_index[round_index]
        size = sizes[round_index]
        data_aligned_lists_by_rounded_size_index[round_index] = AlignedList.new @dir / "size_and_data_of_rounded_size_#{size.to_s.rjust 10, '0'}.dat", size
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

    def initialize(@dir, @logarithmically_divided_sizes_scale)
    end

    def clear
      @data_aligned_lists_by_rounded_size_index.each { |al| al.clear if al }
    end

    alias Id = {rounded_size_index: UInt8, pointer: Int64}

    alias Add = {data: Bytes, data_index: Int32}

    def update(add : Array(Bytes), delete : Array(Id)) : Array(Id)
      ::Log.debug { "RoundDataStorage.update add: #{add.map &.hexstring}, delete: #{delete}" }

      add_data_by_rounded_size_index = Array(Array(Add)?).new(sizes.size) { nil }
      add.each_with_index do |data, data_index|
        size_and_data_encoded = IO::Memory.new
        Lawn.encode_bytes_with_size_size size_and_data_encoded, data
        i = round_index size_and_data_encoded.size
        add_data_by_rounded_size_index[i] = Array(Add).new unless add_data_by_rounded_size_index[i]
        add_data_by_rounded_size_index[i].not_nil! << {data: size_and_data_encoded.to_slice, data_index: data_index}
      end

      delete_pointers_by_rounded_size_index = Array(Array(Int64)?).new(sizes.size) { nil }
      delete.each do |id|
        delete_pointers_by_rounded_size_index[id[:rounded_size_index]] = Array(Int64).new unless delete_pointers_by_rounded_size_index[id[:rounded_size_index]]
        delete_pointers_by_rounded_size_index[id[:rounded_size_index]].not_nil! << id[:pointer]
      end

      r = Array(Id).new(add.size) { {rounded_size_index: 0_u8, pointer: 0_i64} }
      (0_u8..sizes.size - 1).each do |i|
        i_add = add_data_by_rounded_size_index[i]
        i_delete = delete_pointers_by_rounded_size_index[i]
        if i_add || i_delete
          data_aligned_list(i)
            .update(add: (i_add ? i_add.map &.[:data] : [] of Bytes), delete: i_delete)
            .each_with_index { |pointer, add_index| r[i_add.not_nil![add_index][:data_index]] = {rounded_size_index: i, pointer: pointer} }
        end
      end

      ::Log.debug { "RoundDataStorage.update => #{r}" }
      r
    end

    def get(id : Id)
      ::Log.debug { "RoundDataStorage.get #{id}" }

      al = @data_aligned_lists_by_rounded_size_index[id[:rounded_size_index]]
      return unless al

      size_and_data_encoded = IO::Memory.new al.get id[:pointer]
      Lawn.decode_bytes_with_size_size size_and_data_encoded
    end

    def round_index(size : Int32) : Int32
      sizes.bsearch_index { |n| n >= size }.not_nil!
    end
  end
end
