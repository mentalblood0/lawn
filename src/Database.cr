require "yaml"

require "./common"
require "./Transaction"
require "./Log"
require "./Table"

module Lawn
  class Database
    Lawn.mserializable

    getter log : Log
    getter tables : Array(Table)

    def initialize(@log, @tables)
    end

    def after_initialize
      @log.read { |entry| tables[entry[:table_id]].memtable[entry[:keyvalue][0]] = entry[:keyvalue][1] }
      ::Log.debug { "Initialized database #{self.to_json}" }
    end

    def clear
      ::Log.debug { "Database.clear" }
      log.clear
      tables.each { |table| table.clear }
    end

    def transaction
      ::Log.debug { "Database.transaction" }
      Transaction.new self
    end

    def checkpoint
      ::Log.debug { "Database.checkpoint" }
      tables.each { |table| table.checkpoint }
      @log.clear
    end
  end
end
