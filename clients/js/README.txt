
$ npm install rochefort

example:

   var data = new Buffer('asdasljdasjhd')
   var r = new rochefort('http://localhost:8002')
   var stored_offset
   r.append({data: data}).then(offset => {
       // append example returns the stored offset
       // since the offset is 64 bit, rochefort also returns a string (offset_str)
       // and we use the string version
       stored_offset = offset
       return r.get({offset: offset})
   }).then(value => {
       // get examplem returns the stored buffer
       console.log(value)
   }).then(() => {
       // again offsets is array of strings returned from append()
       return r.getMulti({offsets: [stored_offset,stored_offset,stored_offset,stored_offset]})
   }).then(values => {
       // multi get example
       // returns array of Buffers
       console.log(values)
   })
