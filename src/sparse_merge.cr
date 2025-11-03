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

  def self.insert_merge(big : Array(Bytes), small : Array(Bytes))
    comparisons_count = 0
    result = small.map_with_index { |element_to_insert, i| big.bsearch_index do |element, _|
      comparisons_count += 1
      element >= element_to_insert
    end || 0 }
    puts "insert_merge: #{comparisons_count}"
    result
  end

  def self.sparse_merge(big : Array(Bytes), small : Array(Bytes))
    comparisons_count = 0
    result_insert_indexes = Array(Int32?).new(small.size) { nil }
    self.walk_middles(small.size) do |indexes|
      element_to_insert = small[indexes[:middle]]

      left_bound = result_insert_indexes[Math.max 0, indexes[:left] - 1] || 0
      right_bound = result_insert_indexes[Math.min indexes[:right] + 1, result_insert_indexes.size - 1] || big.size - 1
      if left_bound > right_bound
        temp = left_bound
        left_bound = right_bound
        right_bound = temp
      end

      insert_relative_index = Slice(Bytes).new(big.to_unsafe + left_bound, right_bound + 1 - left_bound).bsearch_index do |element, _|
        comparisons_count += 1
        element >= element_to_insert
      end || 0
      insert_index = insert_relative_index + left_bound

      result_insert_indexes[indexes[:middle]] = insert_index
    end
    puts "sparse_merge: #{comparisons_count}"
    result_insert_indexes
  end

  def self.sparse_merge_sequential(big : Array(Bytes), small : Array(Bytes))
    comparisons_count = 0
    result_insert_indexes = Array(Int32?).new(small.size) { nil }

    last_insert_index = 0
    small.each_with_index do |element_to_insert, element_to_insert_index|
      insert_relative_index = Slice(Bytes).new(big.to_unsafe + last_insert_index, big.size - last_insert_index).bsearch_index do |element, _|
        comparisons_count += 1
        element >= element_to_insert
      end || 0
      insert_index = insert_relative_index + last_insert_index
      result_insert_indexes[element_to_insert_index] = insert_index
      last_insert_index = insert_index
    end

    puts "sparse_merge_sequential: #{comparisons_count}"
    result_insert_indexes
  end
end
