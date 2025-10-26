require "./exceptions"
require "./common"
require "./Transaction"
require "./Log"
require "./VariableTable"
require "./FixedTable"

module Lawn
  class Database
    Lawn.mserializable

    getter log : Log
    getter tables : Array(VariableTable | FixedTable)

    Lawn.mignore
    getter transactions = {in_work: Set(Transaction).new, committed: Set(Transaction).new}

    def initialize(@log, @tables)
    end

    def bytesize_disk
      result = log.bytesize
      tables.each { |table| result += table.bytesize_disk }
      result
    end

    def after_initialize
      @log.read(@tables) { |entry| tables[entry[:table_id]].memtable[entry[:keyvalue][0]] = entry[:keyvalue][1] }
    end

    def clear
      ::Log.debug { "#{self.class}.clear" }
      log.clear
      tables.each { |table| table.clear }
      self
    end

    def transaction
      ::Log.debug { "#{self.class}.transaction" }
      Transaction.new self
    end

    protected def commit(transaction : Transaction)
      ::Log.debug { "#{self.class}.commit #{transaction}" }

      @transactions[:committed].each do |committed_transaction|
        if transaction.accessed_keys.intersects? committed_transaction.accessed_keys
          raise Exception.new "Transaction #{transaction} interfere with already committed transaction #{committed_transaction} and therefore can not be committed"
        end
      end

      @log.write @tables, transaction.batches
      transaction.batches.each_with_index do |batch, table_id|
        next unless batch
        table = @tables[table_id]
        batch.each { |key, value| table.memtable[key] = value }
      end

      @transactions[:in_work].delete transaction
      @transactions[:committed] << transaction unless @transactions[:in_work].empty?
      self
    end

    def checkpoint
      ::Log.debug { "#{self.class}.checkpoint" }
      tables.each { |table| table.checkpoint }
      @log.clear
      self
    end
  end
end
