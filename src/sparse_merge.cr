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

  def self.insert_merge(big : Slice(Bytes), small : Array(Bytes), result_insert_indexes : Array(Int32?))
    small.each_with_index { |element_to_insert, i| result_insert_indexes[i] = big.bsearch_index { |element, _| element >= element_to_insert } }
  end

  def self.sparse_merge(big : Slice(Bytes), small : Array(Bytes), result_insert_indexes : Array(Int32?))
    self.walk_middles(small.size) do |indexes|
      puts "indexes = #{indexes}"
      element_to_insert = small[indexes[:middle]]

      left_bound = result_insert_indexes[Math.max 0, indexes[:left] - 1] || 0
      right_bound = result_insert_indexes[Math.min indexes[:right] + 1, result_insert_indexes.size - 1] || big.size - 1
      if left_bound > right_bound
        temp = left_bound
        left_bound = right_bound
        right_bound = temp
      end
      puts "left_bound = #{left_bound}, right_bound = #{right_bound}"

      puts "search in big[#{left_bound}..#{right_bound}]"

      insert_relative_index = big[left_bound..right_bound].bsearch_index { |element, _| element >= element_to_insert } || 0
      insert_index = insert_relative_index + left_bound
      puts "insert_index = #{insert_index}"

      result_insert_indexes[indexes[:middle]] = insert_index
      puts
    end
  end
end
