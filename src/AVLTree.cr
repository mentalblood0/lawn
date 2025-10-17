module Lawn
  class AVLTree
    getter root : Node? = nil

    class Node
      property key : Key
      property value : Value

      property left : Node?
      property right : Node?

      property height : Int64

      def initialize(@key, @value)
        @left = nil
        @right = nil
        @height = 1
      end
    end

    def []=(key : Key, value : Value)
      @root = insert @root, key, value
    end

    def [](key : Key, node : Node? = @root) : Value?
      return unless node

      if key == node.key
        node.value
      elsif key < node.key
        self[key, node.left]
      else
        self[key, node.right]
      end
    end

    def delete(key : Key)
      @root = delete key, @root
    end

    def get_balance(node : Node?)
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
          return node.left || node.right
        else
          temp = min_key_node(node.right).not_nil!

          node.key = temp.key
          node.value = temp.value
          node.right = delete temp.key, node.right
        end
      end

      return node unless node

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

    def scan(node : Node? = @root)
      return unless node

      scan node.left
      ::Log.debug { "scan: #{node.key.hexstring} : #{node.value.hexstring}" }
      scan node.right
    end

    protected def height(node : Node?)
      node ? node.height : 0
    end

    protected def update_height(node : Node)
      node.height = 1 + Math.max height(node.left), height(node.right)
    end

    protected def insert(node : Node?, key : Key, value : Value) : Node
      return Node.new key, value unless node

      if key < node.key
        node.left = insert node.left, key, value
      elsif key > node.key
        node.right = insert node.right, key, value
      else
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
