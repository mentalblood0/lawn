module Lawn
  abstract class DataStorage(I)
    abstract def update(add : Array(Bytes), delete : Array(I)? = nil) : Array(I)
    abstract def get(id : I) : Bytes?
  end
end
