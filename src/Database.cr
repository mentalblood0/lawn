require "./exceptions"
require "./common"
require "./Transaction"
require "./Log"
require "./VariableTable"
require "./FixedTable"

module Lawn
  class Database
    Lawn.serializable

    getter log : Log
    getter tables : Array(VariableTable | FixedTable)

    alias ScansRanges = Set({UInt8, Range(Key?, Key?)})
    alias TransactionInfo = {began_at: Time, committed_at: Time, accessed_keys: {read: Set({UInt8, Key}), write: Set({UInt8, Key})}, scans_ranges: ScansRanges}

    Lawn.mignore
    getter transactions = {
      in_work:   Set(Transaction).new,
      committed: Set(TransactionInfo).new,
    }

    def initialize(@log, @tables)
    end

    def bytesize_disk
      result = log.bytesize
      tables.each { |table| result += table.bytesize_disk }
      result
    end

    def recover
      @log.read(@tables) { |entry| tables[entry[:table_id]].memtable[entry[:keyvalue][0]] = entry[:keyvalue][1] }
    end

    def after_initialize
      recover
    end

    def clear
      ::Log.debug { "#{self.class}.clear" }
      transactions[:in_work].clear
      transactions[:committed].clear
      tables.each { |table| table.clear }
      log.clear
      self
    end

    def transaction
      ::Log.debug { "#{self.class}.transaction" }
      result = Transaction.new self
      transactions[:in_work] << result
      result
    end

    protected def intersects?(accessed_keys : Set(Key), scans_ranges : ScansRanges)
      accessed_keys.any? { |key| scans_ranges.any? { |range| range.includes? key } }
    end

    protected def intersects?(accessed_keys : Set({UInt8, Key}), scans_ranges : ScansRanges)
      accessed_keys.any? { |key_table_id, key| scans_ranges.any? { |scan_table_id, range| key_table_id == scan_table_id && range.includes? key } }
    end

    protected def commit(transaction : Transaction)
      ::Log.debug { "#{self.class}.commit #{transaction}" }
      raise Exception.new "Transaction #{transaction} is orphaned, can not commit" unless @transactions[:in_work].includes? transaction

      scans_ranges = ScansRanges.new
      transaction.cursors.each do |table_id, cursor|
        if cursor_range = cursor.range
          scans_ranges << {table_id, cursor_range}
        end
      end
      @transactions[:committed].each do |committed_transaction|
        if (transaction.began_at < committed_transaction[:committed_at]) &&
           (
             (transaction.accessed_keys[:read].intersects? committed_transaction[:accessed_keys][:write]) ||
             (transaction.accessed_keys[:write].intersects? committed_transaction[:accessed_keys][:write]) ||
             (transaction.accessed_keys[:write].intersects? committed_transaction[:accessed_keys][:read]) ||
             intersects?(committed_transaction[:accessed_keys][:write], scans_ranges) ||
             intersects?(transaction.accessed_keys[:write], committed_transaction[:scans_ranges])
           )
          raise Exception.new "Transaction #{transaction} interfere with already committed transaction #{committed_transaction} and therefore can not be committed"
        end
      end

      @log.write @tables, transaction.changes
      transaction.changes.each_with_index do |batch, table_id|
        table = @tables[table_id]
        batch.cursor.each_next { |key, value| table.memtable[key] = value }
      end

      @transactions[:in_work].delete transaction
      @transactions[:committed] << {began_at: transaction.began_at, committed_at: transaction.committed_at.not_nil!, accessed_keys: transaction.accessed_keys, scans_ranges: scans_ranges}

      @transactions[:committed].each do |committed_transaction|
        unless @transactions[:in_work].any? { |working_transaction| working_transaction.began_at < committed_transaction[:committed_at] }
          @transactions[:committed].delete committed_transaction
        end
      end
      self
    end

    def get(table_id : UInt8, key : Key)
      result_transaction = self.transaction
      result = result_transaction.get(table_id, key)
      result_transaction.commit
      result
    end

    def cursor(table_id : UInt8, from : Key? = nil, including_from : Bool = true, direction = :forward, &)
      transaction = self.transaction
      result = transaction.cursor table_id, from, including_from, direction
      yield result
      transaction.commit
    end

    def checkpoint
      ::Log.debug { "#{self.class}.checkpoint" }
      tables.each { |table| table.checkpoint }
      @log.clear
      self
    end
  end
end
