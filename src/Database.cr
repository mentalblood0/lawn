require "yaml"

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

    def initialize(@log, @tables)
    end

    def bytesize_disk
      result = log.bytesize
      tables.each { |table| result += table.bytesize_disk }
      result
    end

    def after_initialize
      @log.read(@tables) { |entry| tables[entry[:table_id]].memtable[entry[:keyvalue][0]] = entry[:keyvalue][1] }
      ::Log.debug { "Initialized database #{self.to_json}" }
    end

    def clear
      ::Log.debug { "#{self.class}.clear" }
      log.clear
      tables.each { |table| table.clear }
    end

    def transaction
      ::Log.debug { "#{self.class}.transaction" }
      Transaction.new self
    end

    def checkpoint
      ::Log.debug { "#{self.class}.checkpoint" }
      tables.each { |table| table.checkpoint }
      @log.clear
    end
  end
end
