module Lawn
  abstract class DataStorage(I)
    abstract def update(add : Array(Bytes), delete : Array(I)? = nil) : Array(I)
    abstract def get(id : I) : Bytes?
    abstract def bytesize_disk : Int64
  end
end
