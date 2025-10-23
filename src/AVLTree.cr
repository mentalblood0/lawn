require "./exceptions"

module Lawn
  class AVLTree
    getter root : Node? = nil
    getter size : Int32 = 0

    class Node
      property key : Key
      property value : Value?

      property left : Node?
      property right : Node?

      property height : Int8

      def initialize(@key, @value)
        @left = nil
        @right = nil
        @height = 1
      end
    end

    class Cursor
      getter stack = [] of Node
      getter current : Node?
      getter from : Key? = nil

      def initialize(@current, @from = nil)
      end

      def next
        while @current || !@stack.empty?
          while @current
            @stack << @current.not_nil!
            break if @from && (@current.not_nil!.key < @from.not_nil!)
            @current = @current.not_nil!.left
          end
          @current = @stack.pop
          r = {@current.not_nil!.key, @current.not_nil!.value}
          @current = @current.not_nil!.right
          return r unless @from && (r[0] < @from.not_nil!)
        end
      end
    end

    def []=(key : Key, value : Value?)
      @root = upsert @root, key, value
    end

    def []?(key : Key, node : Node? = @root) : Value?
      return unless node

      if key == node.key
        node.value
      elsif key < node.key
        self[key, node.left]?
      else
        self[key, node.right]?
      end
    end

    def each(from : Key? = nil, &)
      cursor = Cursor.new @root, from
      while r = cursor.next
        yield r
      end
    end

    def each(from : Key? = nil)
      r = [] of {Key, Value?}
      each(from) { |keyvalue| r << keyvalue }
      r
    end

    def delete(key : Key)
      @root = delete key, @root
    end

    def clear
      @root = nil
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

    protected def upsert(node : Node?, key : Key, value : Value?) : Node
      unless node
        @size += 1
        return Node.new key, value
      end

      if key < node.key
        node.left = upsert node.left, key, value
      elsif key > node.key
        node.right = upsert node.right, key, value
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
        return rotate_right node
      end
      # right left
      if (balance < -1) && (key < node.right.not_nil!.key)
        node.right = rotate_right node.right.not_nil!
        return rotate_left node
      end

      node
    end

    protected def rotate_right(y : Node) : Node
      x = y.left.not_nil!
      t2 = x.right

      x.right = y
      y.left = t2

      update_height y
      update_height x

      x
    end

    protected def rotate_left(x : Node) : Node
      y = x.right.not_nil!
      t2 = y.left

      y.left = x
      x.right = t2

      update_height x
      update_height y

      y
    end
  end
end
