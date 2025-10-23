require "yaml"
require "json"

require "./common"
require "./exceptions"
require "./AlignedList"
require "./DataStorage"

module Lawn
  class RoundDataStorage < DataStorage({rounded_size_index: UInt8, pointer: Int64})
    Lawn.mserializable

    getter dir : Path
    getter max_element_size : Int32

    Lawn.mignore
    getter data_aligned_lists_by_rounded_size_index : Array(AlignedList?) = Array(AlignedList?).new 256 { nil }

    alias Schema = {max_element_size: Int32}

    Lawn.mignore
    getter schema : Schema { {max_element_size: @max_element_size} }

    Lawn.mignore
    getter sequential_sizes_threshold_index : Int32 { sizes.index { |size| size != sizes[size - 1] } || sizes.size - 1 }

    Lawn.mignore
    getter sequential_sizes_threshold : Int32 { sizes[sequential_sizes_threshold_index] }

    def need_size_if_index?(rounded_size_index : Int32)
      rounded_size_index >= sequential_sizes_threshold_index
    end

    def need_size_if_size?(size : Int32)
      size >= sequential_sizes_threshold
    end

    def data_aligned_list(round_index : UInt8)
      unless data_aligned_lists_by_rounded_size_index[round_index]
        size = @sizes[round_index]
        data_aligned_lists_by_rounded_size_index[round_index] = AlignedList.new @dir / "size_and_data_of_rounded_size_#{size.to_s.rjust 10, '0'}.dat", size
      end
      data_aligned_lists_by_rounded_size_index[round_index].not_nil!
    end

    Lawn.mignore
    getter sizes = [] of Int32

    def initialize(@dir, @max_element_size)
      after_initialize
    end

    def after_initialize
      schema_path = dir / "schema (DO NOT DELETE OR MODIFY).yml"
      if File.exists? schema_path
        raise Exception.new "#{self.class.name}: Config do not match schema in #{schema_path}, can not operate as may corrupt data" if schema != Schema.from_yaml File.read schema_path
      else
        Dir.mkdir_p schema_path.parent
        File.write schema_path, schema.to_yaml
      end
      @sizes = RoundDataStorage.get_sizes max: @max_element_size, points: 2 ** 8
    end

    def self.get_sizes(max : Int32, points : Int32)
      raise Exception.new "Maximum size should be >= points count, but (max = #{max}) <= (points = #{points})" unless max >= points
      rs = Set(Int32).new

      (0..points - 2).each do |i|
        log = Math.log(1) + (Math.log(max) - Math.log(1)) * i / (points - 1)
        r = Math.exp(log).to_i32
        rs << r
      end
      rs << max

      (1..max).each do |i|
        break if rs.size == points
        rs << i
      end

      rs.to_a.sort
    end

    def clear
      @data_aligned_lists_by_rounded_size_index.each { |al| al.clear if al }
    end

    alias Id = {rounded_size_index: UInt8, pointer: Int64}

    alias Add = {data: Bytes, data_index: Int32}

    def update(add : Array(Bytes), delete : Array(Id)? = nil) : Array(Id)
      ::Log.debug { "RoundDataStorage.update add: #{add.map &.hexstring}, delete: #{delete}" }

      add_data_by_rounded_size_index = Array(Array(Add)?).new(@sizes.size) { nil }
      add.each_with_index do |data, data_index|
        if need_size_if_size? data.size
          size_and_data_encoded_io = IO::Memory.new
          Lawn.encode_bytes_with_size_size size_and_data_encoded_io, data
          size_and_data_encoded = size_and_data_encoded_io.to_slice
        else
          size_and_data_encoded = data
        end
        i = round_index size_and_data_encoded.size
        add_data_by_rounded_size_index[i] = Array(Add).new unless add_data_by_rounded_size_index[i]
        add_data_by_rounded_size_index[i].not_nil! << {data: size_and_data_encoded, data_index: data_index}
      end

      delete_pointers_by_rounded_size_index = Array(Array(Int64)?).new(@sizes.size) { nil }
      if delete
        delete.each do |id|
          delete_pointers_by_rounded_size_index[id[:rounded_size_index]] = Array(Int64).new unless delete_pointers_by_rounded_size_index[id[:rounded_size_index]]
          delete_pointers_by_rounded_size_index[id[:rounded_size_index]].not_nil! << id[:pointer]
        end
      end

      r = Array(Id).new(add.size) { {rounded_size_index: 0_u8, pointer: 0_i64} }
      (0_u8..@sizes.size - 1).each do |i|
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

    def get(id : Id) : Bytes?
      ::Log.debug { "RoundDataStorage.get #{id}" }

      al = data_aligned_list id[:rounded_size_index]
      return unless al

      size_and_data_encoded = al.get id[:pointer]
      if need_size_if_index? id[:rounded_size_index]
        Lawn.decode_bytes_with_size_size IO::Memory.new size_and_data_encoded
      else
        size_and_data_encoded
      end
    end

    def round_index(size : Int32) : Int32
      @sizes.bsearch_index { |n| n >= size }.not_nil!
    end
  end
end
