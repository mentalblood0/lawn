require "log"
require "spec"

require "../src/Database"
require "../src/RoundDataStorage"
require "../src/AVLTree"
require "../src/sparse_merge"

VARIABLE_TABLE      = 0_u8
FIXED_TABLE         = 1_u8
FIXED_KEYONLY_TABLE = 2_u8

struct Slice(T)
  def pretty_print(pp : PrettyPrint)
    pp.text "Bytes[#{self.hexstring}]"
  end
end

alias Config = {database: Lawn::Database, seed: Int32}

config = Config.from_yaml File.read ENV["SPEC_CONFIG_PATH"]
rnd = Random.new config[:seed]

describe "Lawn.sparse_merge" do
  it "merges correctly", focus: true do
    big = Array(Bytes).new(100) { rnd.random_bytes 16 }
    small = Array(Bytes).new(10) { rnd.random_bytes 16 }

    big.sort!
    small.sort!

    slice = Slice(Bytes).new(big.to_unsafe, big.size)

    correct_result_insert_indexes = Array(Int32?).new(small.size) { nil }
    Lawn.insert_merge slice, small, correct_result_insert_indexes

    result_insert_indexes = Array(Int32?).new(small.size) { nil }
    result = Lawn.sparse_merge slice, small, result_insert_indexes

    result_insert_indexes.should eq correct_result_insert_indexes
  end
end

Spec.before_each { config[:database].clear }

describe Lawn do
  it "encodes/decodes numbers encoded in arbitrary number of bytes" do
    io = IO::Memory.new
    (1_u8..8).each do |size_size|
      (0_u64..Math.min 2_u64 ** 12, 2_u64 ** ((size_size == 8) ? 63 : (size_size * 8)) - 2).each do |size|
        Lawn.encode_number io, size, size_size
        io.rewind
        (Lawn.decode_number io, size_size).should eq size
        io.rewind
      end
    end
  end
  it "encodes/decodes bytes with sizes" do
    io = IO::Memory.new
    Lawn.encode_bytes_with_size io, "lalala".to_slice, 1_u8
    io.rewind
    (Lawn.decode_bytes_with_size io, 1_u8).should eq "lalala".to_slice
    io.rewind
    Lawn.encode_bytes_with_size io, "lalala".to_slice, 3_u8
    io.rewind
    (Lawn.decode_bytes_with_size io, 3_u8).should eq "lalala".to_slice
  end
end

describe Lawn::AlignedList do
  it "correctly deletes all the elements" do
    amount = 16
    element_size = 16
    aligned_list = Lawn::AlignedList.new config[:database].log.path.parent / "aligned_list.dat", element_size
    data = Array(Bytes).new(amount) { rnd.random_bytes element_size }

    ids = aligned_list.update data
    ids.each_with_index { |id, data_index| aligned_list.get(id).should eq data[data_index] }

    aligned_list.update [] of Bytes, ids

    ids = aligned_list.update data
    ids.each_with_index { |id, data_index| aligned_list.get(id).should eq data[data_index] }

    aligned_list.file.delete
  end

  [2, 3, 5, 9].map { |s| s.to_u8! }.each do |s|
    it "generative test: supports #{s} bytes elements" do
      data_path = config[:database].log.path.parent / "aligned_list.dat"
      aligned_list = Lawn::AlignedList.new data_path, s
      added = Hash(Int64, Bytes).new

      1000.times do
        add = Array.new(rnd.rand 1..16) { rnd.random_bytes s }
        delete = added.keys.sample rnd.rand(1..16), rnd
        delete.each { |pointer| added.delete pointer } if delete
        result = aligned_list.update add, delete
        result.each_with_index { |pointer, add_index| added[pointer] = add[add_index] }
      end
      aligned_list.bytesize_disk.should eq aligned_list.file.size
      aligned_list = Lawn::AlignedList.new data_path, s
      added.each do |i, b|
        (aligned_list.get i).should eq b
      end
      aligned_list.file.delete
    end
  end
end

data_storage = config[:database].tables.first.data_storage
case data_storage
when Lawn::RoundDataStorage
  describe Lawn::RoundDataStorage do
    it "correctly splits sizes scale" do
      points = 256
      (256..2**(8 * 2)).step(19).each do |max|
        r = Lawn::RoundDataStorage.get_sizes max, points
        r.sort.should eq r
        r.last.should eq max
        r.size.should eq points
      end
    end

    it "simple test" do
      add = Array(Bytes).new(2) { rnd.random_bytes rnd.rand 1..16 }
      (data_storage.update add, [] of Lawn::RoundDataStorage::Id).each_with_index { |id, data_index| data_storage.get(id).should eq add[data_index] }
    end

    it "generative test" do
      added = Hash(Lawn::RoundDataStorage::Id, Bytes).new
      1000.times do
        add = Array(Bytes).new(rnd.rand 1..16) { rnd.random_bytes rnd.rand 1..1024 }
        delete = added.keys.sample rnd.rand(1..16), rnd
        r = data_storage.update add, delete
        r.each_with_index { |pointer, data_index| added[pointer] = add[data_index] }
        delete.each { |pointer| added.delete pointer }
      end
      added.each do |pointer, data|
        (data_storage.get pointer).should eq data
      end
    end
  end
end

describe Lawn::AVLTree do
  it "distinguishes between absent key and key with absent value" do
    tree = Lawn::AVLTree.new
    tree["key".to_slice] = nil
    tree["key".to_slice]?.should eq nil
    tree["absent_key".to_slice]?.should eq :no_key
  end

  it "can be traversed on pair with another AVLTree" do
    added = Hash(Bytes, Bytes?).new

    tree_b = Lawn::AVLTree.new
    100.times do
      key = rnd.random_bytes rnd.rand 1..2
      value = (rnd.rand(0..1) == 1) ? rnd.random_bytes(rnd.rand(1..1024)) : nil
      added[key] = value
      tree_b[key] = value
    end

    tree_a = Lawn::AVLTree.new
    100.times do
      key = rnd.random_bytes rnd.rand 1..2
      value = (rnd.rand(0..1) == 1) ? rnd.random_bytes(rnd.rand(1..1024)) : nil
      added[key] = value
      tree_a[key] = value
    end

    sorted = added.to_a.sort_by { |key, _| key }
    sorted.each { |key, value| Lawn::Cursor.new(tree_a, tree_b, from: key).all_next.should eq sorted[sorted.index({key, value})..] }
    sorted.each { |key, value| Lawn::Cursor.new(tree_a, tree_b, from: key, including_from: false).all_next.should eq sorted[sorted.index({key, value}).not_nil! + 1..] }
    sorted.reverse!
    sorted.each { |key, value| Lawn::Cursor.new(tree_a, tree_b, from: key, direction: :backward).all_next.should eq sorted[sorted.index({key, value})..] }
    sorted.each { |key, value| Lawn::Cursor.new(tree_a, tree_b, from: key, including_from: false, direction: :backward).all_next.should eq sorted[sorted.index({key, value}).not_nil! + 1..] }
  end

  it "generative test" do
    tree = Lawn::AVLTree.new
    added = Hash(Bytes, Bytes?).new
    1000.times do
      case rnd.rand 0..2
      when 0, 1, 2
        key = rnd.random_bytes rnd.rand 1..8
        value = rnd.random_bytes rnd.rand 1..8
        ::Log.debug { "add #{key.hexstring} : #{value.hexstring}" }
        tree[key] = value
        added[key] = value
      when 3
        key = added.keys.sample rnd rescue next
        ::Log.debug { "delete #{key.hexstring}" }
        tree.delete key
        added.delete key
      when 4
        key = added.keys.sample rnd rescue next
        ::Log.debug { "set value = nil #{key.hexstring}" }
        tree[key] = nil
        added[key] = nil
      end
      added.each { |key, value| tree[key]?.should eq value }
      tree.size.should eq added.size
    end
    sorted = added.to_a.sort_by { |key, _| key }
    tree.cursor.all_next.should eq sorted
    sorted.each { |key, value| Lawn::AVLTree::Cursor.new(tree.root, from: key).all_next.should eq sorted[sorted.index({key, value})..] }
    sorted.each { |key, value| Lawn::AVLTree::Cursor.new(tree.root, from: key, including_from: false).all_next.should eq sorted[sorted.index({key, value}).not_nil! + 1..] }
    sorted.reverse!
    sorted.each { |key, value| Lawn::AVLTree::Cursor.new(tree.root, from: key, direction: :backward).all_next.should eq sorted[sorted.index({key, value})..] }
    sorted.each { |key, value| Lawn::AVLTree::Cursor.new(tree.root, from: key, including_from: false, direction: :backward).all_next.should eq sorted[sorted.index({key, value}).not_nil! + 1..] }
  end
end

macro test_scans
  added.each_with_index do |added_in_table, table_id|
    added_in_table.keys.each { |k| database.get(table_id.to_u8, k).should eq added_in_table[k] }
  end
  added.each_with_index do |added_in_table, table_id|
    table_id = table_id.to_u8
    all_added_in_table = added_in_table.to_a.sort_by { |key, _| key }
    transaction = database.transaction
    all_present_in_table = transaction.cursor(table_id).all_next
    transaction.commit
    all_present_in_table.should eq all_added_in_table
    all_present_in_table.each do |key, value|
      database.cursor(table_id, from: key) { |cursor| cursor.all_next.should eq all_added_in_table[all_added_in_table.index({key, value})..] }
      database.cursor(table_id, from: key, including_from: false) { |cursor| cursor.all_next.should eq all_added_in_table[all_added_in_table.index({key, value}).not_nil! + 1..] }
    end
    all_added_in_table.reverse!
    all_present_in_table.each do |key, value|
      database.cursor(table_id, from: key, direction: :backward) { |cursor| cursor.all_next.should eq all_added_in_table[all_added_in_table.index({key, value})..] }
      database.cursor(table_id, from: key, including_from: false, direction: :backward) { |cursor| cursor.all_next.should eq all_added_in_table[all_added_in_table.index({key, value}).not_nil! + 1..] }
    end
  end
end

describe Lawn::Database do
  database = config[:database]

  it "recovers from log" do
    key = "1234567890abcdef".to_slice
    database.transaction.set(FIXED_KEYONLY_TABLE, key).commit
    database.tables[FIXED_KEYONLY_TABLE].memtable.clear
    database.recover
    database.get(FIXED_KEYONLY_TABLE, key).should eq Bytes.new 0
  end

  it "checkpoints" do
    key = "key".to_slice
    value = "value".to_slice
    database.transaction.set(VARIABLE_TABLE, key, value).commit
    database.checkpoint
    database.get(VARIABLE_TABLE, key).should eq value
  end

  it "handles deletes correctly" do
    database
      .transaction
      .set(VARIABLE_TABLE, "key_to_delete".to_slice, "value".to_slice)
      .set(VARIABLE_TABLE, "key".to_slice, "value".to_slice)
      .commit
      .checkpoint
    database.get(VARIABLE_TABLE, "key_to_delete".to_slice).should eq "value".to_slice
    database.get(VARIABLE_TABLE, "key".to_slice).should eq "value".to_slice

    database.transaction.delete(VARIABLE_TABLE, "key_to_delete".to_slice).commit
    database.checkpoint
    database.get(VARIABLE_TABLE, "key_to_delete".to_slice).should eq nil
    database.get(VARIABLE_TABLE, "key".to_slice).should eq "value".to_slice
  end

  it "handles in-memory deletes correctly" do
    key = "1234567890abcdef".to_slice
    database.transaction.set(FIXED_KEYONLY_TABLE, key).commit.checkpoint
    database.cursor(FIXED_KEYONLY_TABLE) { |cursor| cursor.all_next.should eq [{key, Bytes.new 0}] }
    database
      .transaction.delete(FIXED_KEYONLY_TABLE, key).commit
      .get(FIXED_KEYONLY_TABLE, key).should eq nil
    database.cursor(FIXED_KEYONLY_TABLE) { |cursor| cursor.all_next.should eq [] of Lawn::KeyValue }
  end

  it "handles updates correctly" do
    key = "key".to_slice
    value = "value".to_slice
    new_value = "new_value".to_slice

    database.transaction.set(VARIABLE_TABLE, key, value).commit
    database.checkpoint
    database.get(VARIABLE_TABLE, key).should eq value

    database.transaction.set(VARIABLE_TABLE, key, new_value).commit
    database.checkpoint
    database.get(VARIABLE_TABLE, key).should eq new_value
  end

  it "handles wide range of sizes correctly" do
    value = "v".to_slice
    keyvalues = (1..1024).map { |key_size| {("k" * key_size).to_slice, value} }
    database.transaction.set(VARIABLE_TABLE, keyvalues).commit.checkpoint
    keyvalues.each { |key, value| database.get(VARIABLE_TABLE, key).should eq value }
  end

  it "supports setting empty values" do
    key = "1234567890abcdef".to_slice
    database.transaction.set(FIXED_KEYONLY_TABLE, key).commit
    database.get(FIXED_KEYONLY_TABLE, key).should eq Bytes.new 0
    database.checkpoint
    database.get(FIXED_KEYONLY_TABLE, key).should eq Bytes.new 0
  end

  it "handles prefix search" do
    key = "1234567890abcdef".to_slice
    prefix = key[..4]
    database.transaction.set(FIXED_KEYONLY_TABLE, key).commit
    cursor = database.cursor(FIXED_KEYONLY_TABLE, from: prefix) { |cursor| cursor.next.should eq({key, Bytes.new 0}) }
  end

  it "denies committing orphaned transactions" do
    transaction = database.transaction
    database.clear
    expect_raises(Lawn::Exception) { transaction.commit }
  end

  it "denies committing transactions repeatedly" do
    transaction = database.transaction
    transaction.commit
    expect_raises(Lawn::Exception) { transaction.commit }
  end

  it "immediately raises exception on set with incorrect key/value size" do
    database.transaction do |transaction|
      expect_raises(Lawn::Exception) { transaction.set FIXED_KEYONLY_TABLE, "1234".to_slice }
      expect_raises(Lawn::Exception) { transaction.set FIXED_KEYONLY_TABLE, "1234567890abcdef".to_slice, "1".to_slice }
    end
  end

  it "cancels transaction on exception in transaction block" do
    database.transaction do |transaction|
      raise "oh no"
    end
    database.transactions[:in_work].empty?.should eq true
    database.transactions[:committed].empty?.should eq true
  end

  describe "transactions isolation" do
    key = "1234567890abcdef".to_slice

    it "denies commit if write interfere with committed write" do
      transaction_A = database.transaction
      transaction_B = database.transaction

      transaction_A.set FIXED_KEYONLY_TABLE, key
      transaction_B.set FIXED_KEYONLY_TABLE, key

      transaction_A.commit
      expect_raises(Lawn::Exception) { transaction_B.commit }
    end

    it "denies commit if write interfere with committed read" do
      transaction_A = database.transaction
      transaction_B = database.transaction

      transaction_A.get FIXED_KEYONLY_TABLE, key
      transaction_B.set FIXED_KEYONLY_TABLE, key

      transaction_A.commit
      expect_raises(Lawn::Exception) { transaction_B.commit }
    end

    it "denies commit if read interfere with committed write" do
      transaction_A = database.transaction
      transaction_B = database.transaction

      transaction_A.set FIXED_KEYONLY_TABLE, key
      transaction_B.get FIXED_KEYONLY_TABLE, key

      transaction_A.commit
      expect_raises(Lawn::Exception) { transaction_B.commit }
    end

    it "denies read if it interferes with committed write" do
      transaction_A = database.transaction
      transaction_B = database.transaction

      transaction_A.set FIXED_KEYONLY_TABLE, key
      transaction_A.commit

      expect_raises(Lawn::Exception) { transaction_B.get FIXED_KEYONLY_TABLE, key }
    end

    it "denies write if it interferes with committed write" do
      transaction_A = database.transaction
      transaction_B = database.transaction

      transaction_A.set FIXED_KEYONLY_TABLE, key
      transaction_A.commit

      expect_raises(Lawn::Exception) { transaction_B.set FIXED_KEYONLY_TABLE, key }
    end

    it "does not deny commit if read interfere with committed read" do
      transaction_A = database.transaction
      transaction_B = database.transaction

      transaction_A.get FIXED_KEYONLY_TABLE, key
      transaction_B.get FIXED_KEYONLY_TABLE, key

      transaction_A.commit
      transaction_B.commit
    end

    it "allows transaction see it's changes in gets" do
      database.transaction.set(FIXED_KEYONLY_TABLE, key).get(FIXED_KEYONLY_TABLE, key).should eq Bytes.new 0
    end

    it "allows transaction see it's changes in scans" do
      database.transaction.set(FIXED_KEYONLY_TABLE, key).cursor(FIXED_KEYONLY_TABLE).all_next.should eq [{key, Bytes.new 0}]
    end

    it "denies commit if write interfere with committed range scan" do
      database.transaction.set(FIXED_KEYONLY_TABLE, key).commit

      transaction_A = database.transaction
      transaction_B = database.transaction

      transaction_A.cursor(FIXED_KEYONLY_TABLE, key).all_next
      transaction_B.set FIXED_KEYONLY_TABLE, key

      transaction_A.commit
      expect_raises(Lawn::Exception) { transaction_B.commit }
    end
  end

  it "generative test" do
    added = Array(Hash(Lawn::Key, Lawn::Value)).new(database.tables.size) { Hash(Lawn::Key, Lawn::Value).new }
    100.times do
      database.transaction do |transaction|
        rnd.rand(1..16).times do
          table_id = rnd.rand(0..(database.tables.size - 1)).to_u8

          case table = database.tables[table_id]
          when Lawn::VariableTable
            key_size = rnd.rand 1..16
            value_size = rnd.rand 1..16
          when Lawn::FixedTable
            key_size = table.key_size
            value_size = table.value_size
          else
            raise "unreacheable"
          end

          case rnd.rand 0..2
          when 0, 1
            key = rnd.random_bytes key_size
            value = rnd.random_bytes value_size

            transaction.set table_id, key, value
            added[table_id][key] = value
          when 2
            key = added[table_id].keys.sample rnd rescue next

            transaction.delete table_id, key
            added[table_id].delete key
          end
        end
      end
      test_scans
      database.checkpoint
      added.each_with_index { |added_in_table, table_id| database.tables[table_id].index.size.should eq added_in_table.size }
    end
    test_scans
    database.log.bytesize.should eq database.log.file.size
  end
end
