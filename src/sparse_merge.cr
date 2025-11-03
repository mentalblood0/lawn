module Lawn
  def self.walk_middles(array_size, &)
    queue = Deque(Tuple(Int32, Int32)).new
    queue.push({0, array_size - 1})

    while !queue.empty?
      left_index, right_index = queue.shift
      middle_index = (left_index + right_index) // 2
      yield({left: left_index, middle: middle_index, right: right_index})

      if left_index <= middle_index - 1
        queue.push({left_index, middle_index - 1})
      end

      if middle_index + 1 <= right_index
        queue.push({middle_index + 1, right_index})
      end
    end
  end

  def self.insert_merge(big_size : Int64, big_get_element : Proc(Int64, Bytes), small : Array(Bytes))
    comparisons_count = 0
    result = small.map_with_index { |element_to_insert, i| (0_i64..big_size - 1).bsearch do |i|
      comparisons_count += 1
      big_get_element.call(i) >= element_to_insert
    end || 0_i64 }
    puts "insert_merge: #{comparisons_count}"
    result
  end

  def self.sparse_merge(big_size : Int64, big_get_element : Proc(Int64, Bytes), small : Array(Bytes))
    comparisons_count = 0
    result_insert_indexes = Array(Int32?).new(small.size) { nil }
    self.walk_middles(small.size) do |indexes|
      element_to_insert = small[indexes[:middle]]

      left_bound = (result_insert_indexes[Math.max 0, indexes[:left] - 1] || 0).to_i64!
      right_bound = (result_insert_indexes[Math.min indexes[:right] + 1, result_insert_indexes.size - 1] || big_size - 1).to_i64!

      insert_index = (left_bound..right_bound).bsearch do |i|
        comparisons_count += 1
        big_get_element.call(i) >= element_to_insert
      end || left_bound

      result_insert_indexes[indexes[:middle]] = insert_index.to_i32!
    end
    puts "sparse_merge: #{comparisons_count}"
    result_insert_indexes
  end
end
