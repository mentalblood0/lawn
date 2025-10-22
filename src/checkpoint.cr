module Lawn
  macro def_checkpoint(id_type, id_pointer_field_name)
    def checkpoint
      ::Log.debug { "Database.checkpoint" }
      return self if @memtable.empty?

      global_i = 0_i64
      new_index_positions = [] of Int64
      new_index_pointer_size = 1_u8

      data_to_add = Array(Bytes).new
      ids_to_delete = Set({{id_type}}).new

      index_current = nil
      memtable_cursor = AVLTree::Cursor.new @memtable.root
      memtable_current = memtable_cursor.next
      last_key_yielded_from_memtable = nil

      @index.each do |index_id|
        index_current = get_data index_id
        while memtable_current && (memtable_current[0] <= index_current[0])
          last_key_yielded_from_memtable = memtable_current[0]
          if memtable_current[1]
            data_to_add << encode({memtable_current[0], memtable_current[1].not_nil!})
            new_index_positions << global_i
            global_i += 1
          else
          end
          memtable_current = memtable_cursor.next
        end
        if index_current[0] == last_key_yielded_from_memtable
          ids_to_delete << index_id
        else
          index_id_pointer_size = Lawn.number_size index_id{% if id_pointer_field_name == nil %}{% else %}[:{{id_pointer_field_name}}]{% end %}
          new_index_pointer_size = Math.max new_index_pointer_size, index_id_pointer_size
          global_i += 1
        end
      end
      while memtable_current
        if memtable_current[1]
          data_to_add << encode({memtable_current[0], memtable_current[1].not_nil!})
          new_index_positions << global_i
          global_i += 1
        else
        end
        memtable_current = memtable_cursor.next
      end

      new_index_ids = data_storage.update add: data_to_add, delete: ids_to_delete.to_a
      unless new_index_ids.empty?
        new_index_pointer_size = Math.max new_index_pointer_size, new_index_ids.max_of { |index_id| Lawn.number_size index_id{% if id_pointer_field_name == nil %}{% else %}[:{{id_pointer_field_name}}]{% end %} }
      end

      new_index_file = File.new "#{@index.file.path}.new", "w"
      new_index_file.sync = true
      new_index_file.write_byte new_index_pointer_size
      global_i = 0_i64
      new_index_ids_i = 0
      @index.file.pos = 1
      new_index_positions.each do |new_index_position|
        while global_i < new_index_position
          old_index_id = @index.read
          unless ids_to_delete.includes? old_index_id
            encode new_index_file, old_index_id, new_index_pointer_size
            global_i += 1
          end
        end
        new_index_id = new_index_ids[new_index_ids_i]
        encode new_index_file, new_index_id, new_index_pointer_size
        new_index_ids_i += 1
        global_i += 1
      end
      while ((old_index_id = @index.read) rescue nil)
        unless ids_to_delete.includes? old_index_id
          encode new_index_file, old_index_id, new_index_pointer_size
        end
      end

      new_index_file.rename @index.file.path
      new_index_file.close

      @memtable.clear
      @index = Index.new Path.new(new_index_file.path), @index.cache_size

      self
    end
  end
end
