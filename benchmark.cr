require "./src/Env"
require "./src/common"

alias Result = {data_speed: String, records_speed: String, time: String}

class Benchmarks
  Lawn.mserializable

  getter env : Lawn::Env
  getter seed : Int32
  getter amount : UInt64
  getter key_size : UInt64
  getter value_size : UInt64

  @[YAML::Field(ignore: true)]
  getter results : Hash(String, Result) = {} of String => Result

  @[YAML::Field(ignore: true)]
  getter kv : Hash(Bytes, Bytes) = {} of Bytes => Bytes

  def add(name : String, time : Time::Span)
    bites_written = @amount * (2 + @key_size + 2 + @value_size)
    @results[name] = {data_speed:    "#{(bites_written / time.total_seconds).to_u64.humanize_bytes}/s",
                      records_speed: "#{(@amount / time.total_seconds).to_u64.humanize}r/s",
                      time:          "#{time.total_seconds.humanize}s passed"}
  end

  def benchmark_write
    rnd = Random.new @seed
    @kv.clear
    @amount.times { kv[rnd.random_bytes 16] = rnd.random_bytes 32 }
    add "Env.write #{kv.size} key-value pairs of total size #{kv.map { |k, v| k.size + v.size }.sum.humanize_bytes}", Time.measure { kv.each { |k, v| env.transaction.set(k, v).commit } }
  end

  def benchmark_get
    rnd = Random.new @seed
    ks = @kv.keys
    ks.shuffle! rnd
    add "Env.get #{kv.size} key-value pairs of total size #{kv.map { |k, v| k.size + v.size }.sum.humanize_bytes}", Time.measure { ks.each { |k| env.get k } }
  end

  def benchmark_split_data_storage
    rnd = Random.new @seed
    sds = @env.split_data_storage
    data = Array.new(@amount * 2) { rnd.random_bytes rnd.rand 1..1024 }
    add "SplitDataStorage.add #{amount} data of total size #{(data.map &.size).sum.humanize_bytes}", Time.measure { data.each { |d| sds.add d } }
  end
end

benchmarks = Benchmarks.from_yaml File.read ENV["BENCHMARK_CONFIG_PATH"]
# benchmarks.benchmark_write
# benchmarks.benchmark_get
benchmarks.benchmark_split_data_storage

puts benchmarks.results.to_yaml
