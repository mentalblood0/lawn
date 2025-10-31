require "./exceptions"

module Lawn
  class Cursor
    getter cursor_a : AVLTree::Cursor | Cursor
    getter cursor_b : AVLTree::Cursor | Cursor

    getter started : Bool = false
    getter last_key_yielded_from_a : Key? = nil
    getter current : {Key, Value?}? = nil
    getter direction : Symbol

    def initialize(tree_a : AVLTree, tree_b : AVLTree, from : Key?, including_from : Bool = true, @direction = :forward)
      @cursor_a = tree_a.cursor from, including_from, @direction
      @cursor_b = tree_b.cursor from, including_from, @direction
    end

    def next : {Key, Value?}?
      if !@started
        @started = true
        @cursor_a.next
        @cursor_b.next
      end
      loop do
        current_a = @cursor_a.current
        current_b = @cursor_b.current

        if !current_a && !current_b
          @current = nil
          break
        end

        if current_a && (!current_b || (current_a[0] <= current_b[0]))
          @last_key_yielded_from_a = current_a[0]
          @cursor_a.next
          @current = current_a
          break
        end

        @cursor_b.next
        if current_b && (current_b[0] != @last_key_yielded_from_a)
          @current = current_b
          break
        end
      end
      @current
    end

    def each_next(&)
      while (next_keyvalue = self.next)
        yield next_keyvalue
      end
    end

    def all_next : Array({Key, Value?})
      result = [] of {Key, Value?}
      each_next { |next_keyvalue| result << next_keyvalue }
      result
    end
  end

  class AVLTree
    getter root : Node? = nil
    getter size : Int32 = 0

    class Node
      Lawn.serializable

      property key : Key
      property value : Value?

      property left : Node? = nil
      property right : Node? = nil

      property parent : Node? = nil

      property height : Int8 = 1

      def initialize(@key, @value)
      end
    end

    class Cursor
      getter current_node : Node?
      getter from : Key? = nil
      getter including_from : Bool
      getter direction : Symbol

      def current : {Key, Value?}?
        (current_node_temp = @current_node) ? {current_node_temp.key, current_node_temp.value} : nil
      end

      def initialize(@current_node, @from = nil, @including_from = true, @direction = :forward)
        current_node_temp = @current_node
        ::Log.debug { "#{self.class}.initialize current_node: #{current_node_temp ? current_node_temp.key.hexstring : nil} from: #{from ? from.hexstring : nil}, including_from: #{including_from}, direction: #{@direction}" }
        position
      end

      protected def position
        puts "position to #{@from}"
        return unless from_temp = @from
        while current_node_temp = @current_node
          if from_temp > current_node_temp.key
            break unless left = current_node_temp.left
            @current_node = left
          elsif from_temp < current_node_temp.key
            break unless right = current_node_temp.right
            @current_node = right
          else
            break
          end
        end
      end

      def next : {Key, Value?}?
        ::Log.debug { "#{self.class}.next" }
        puts "next"
        if current_node_temp = @current_node
          puts "@current_node == #{current_node.to_yaml}"
          gets
          case @direction
          when :forward
            if right = current_node_temp.right
              puts "right"
              current_node_temp = right
              @current_node = current_node_temp
            else
              puts "parent"
              loop do
                parent = current_node_temp.parent
                puts "parent = #{parent ? Base64.encode parent.key : nil}"
                break unless parent
                break unless parent_right = parent.right
                if parent_right == current_node_temp
                  puts "current_node_temp = #{parent ? Base64.encode parent.key : nil}"
                  current_node_temp = parent
                else
                  puts "@current_node = #{parent ? Base64.encode parent.key : nil}"
                  @current_node = parent
                  return current
                end
              end
              @current_node = nil
            end
          end
        end
        current
      end

      def each_next(&)
        while (next_keyvalue = self.next)
          yield next_keyvalue
        end
      end

      def all_next : Array({Key, Value?})
        result = [] of {Key, Value?}
        each_next { |next_keyvalue| result << next_keyvalue }
        result
      end
    end

    def cursor(from : Key? = nil, including_from : Bool = true, direction : Symbol = :forward)
      Cursor.new @root, from, including_from, direction
    end

    def []=(key : Key, value : Value?)
      ::Log.debug { "#{self.class}[#{key.hexstring}] = #{value ? value.hexstring : nil}" }
      @root = upsert @root, key, value
      return unless root_temp = @root
      update_left_child_parent root_temp
      update_right_child_parent root_temp
      root_temp.parent = nil
    end

    def []?(key : Key, node : Node? = @root) : Value? | Symbol
      ::Log.debug { "#{self.class}[#{key.hexstring}]?" }
      return :no_key unless node

      if key == node.key
        node.value
      elsif key < node.key
        self[key, node.left]?
      else
        self[key, node.right]?
      end
    end

    def delete(key : Key)
      ::Log.debug { "#{self.class}.delete #{key.hexstring}" }
      @root = delete key, @root
    end

    def clear
      ::Log.debug { "#{self.class}.clear" }
      @root = nil
      @size = 0
    end

    def empty?
      @root == nil
    end

    protected def get_balance(node : Node?)
      node ? height(node.left) - height(node.right) : 0
    end

    protected def min_key_node(node : Node?) : Node?
      current = node
      while current && current.left
        current = current.left
      end
      current
    end

    protected def max_key_node(node : Node?) : Node?
      current = node
      while current && current.right
        current = current.right
      end
      current
    end

    protected def delete(key : Key, node : Node?) : Node?
      return nil unless node

      if key < node.key
        node.left = delete key, node.left
      elsif key > node.key
        node.right = delete key, node.right
      else
        if node.left.nil? || node.right.nil?
          @size -= 1
          return node.left || node.right
        else
          temp = min_key_node(node.right).not_nil!

          node.key = temp.key
          node.value = temp.value
          node.right = delete temp.key, node.right
        end
      end

      return nil unless node

      update_height node

      balance = get_balance node
      if balance > 1
        if get_balance(node.left) >= 0
          return rotate_right node
        else
          node.left = rotate_left(node.left.not_nil!)
          return rotate_right node
        end
      elsif balance < -1
        if get_balance(node.right) <= 0
          return rotate_left node
        else
          node.right = rotate_right node.right.not_nil!
          return rotate_left node
        end
      end

      node
    end

    protected def height(node : Node?)
      node ? node.height : 0
    end

    protected def update_height(node : Node)
      node.height = 1_i8 + Math.max height(node.left), height(node.right)
    end

    protected def update_left_child_parent(node : Node)
      return unless left = node.left
      left.parent = node
    end

    protected def update_right_child_parent(node : Node)
      return unless right = node.right
      right.parent = node
    end

    def test_children_parents(node : Node = @root.not_nil!)
      if left = node.left
        left.parent.should eq node
        test_children_parents left
      end
      if right = node.right
        right.parent.should eq node
        test_children_parents right
      end
    end

    protected def upsert(node : Node?, key : Key, value : Value?) : Node
      unless node
        @size += 1
        return Node.new key, value
      end

      if key < node.key
        node.left = upsert node.left, key, value
        update_left_child_parent node
      elsif key > node.key
        node.right = upsert node.right, key, value
        update_right_child_parent node
      else
        node.value = value
        return node
      end

      update_height node

      balance = get_balance node

      # left left
      return rotate_right node if balance > 1 && key < node.left.not_nil!.key
      # right right
      return rotate_left node if balance < -1 && key > node.right.not_nil!.key

      # left right
      if (balance > 1) && (key > node.left.not_nil!.key)
        node.left = rotate_left node.left.not_nil!
        update_left_child_parent node
        return rotate_right node
      end
      # right left
      if (balance < -1) && (key < node.right.not_nil!.key)
        node.right = rotate_right node.right.not_nil!
        update_right_child_parent node
        return rotate_left node
      end

      node
    end

    protected def rotate_right(y : Node) : Node
      x = y.left.not_nil!
      t2 = x.right

      x.right = y
      update_right_child_parent x
      y.left = t2
      update_left_child_parent y

      update_height y
      update_height x

      x
    end

    protected def rotate_left(x : Node) : Node
      y = x.right.not_nil!
      t2 = y.left

      y.left = x
      update_left_child_parent y
      x.right = t2
      update_right_child_parent x

      update_height x
      update_height y

      y
    end
  end
end
