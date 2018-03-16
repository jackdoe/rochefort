** works with rochefort < 2.0 **

$ gem install rochefort

example usage:


  require 'rochefort'
  
  r = Rochefort.new("http://localhost:8001")
  
  # append data
  offset = r.append(id: '123',data: 'example data')
  offset2 = r.append(id: '123',data: 'some other data')
  
  # get the data stored at offset
  fetched = r.get(offset: offset)
  
  # get multi
  many = r.getMulti(offsets: [offset,offset2])

timeouts:
you can pass read_timeout and open_timeout

if you want to namespace your ids into different directories use the
parameter namespace

e.g.:
  offset = r.append(namespace: 'today', id: '123',data: 'example data')
