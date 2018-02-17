require 'rest-client'
require 'uri'
require 'json'

# client for https://github.com/jackdoe/rochefort - disk speed append + offset service (poor man's kafka), 
# example usage
#  r = new Rochefort("http://localhost:8001")
#  offset = r.append("example-namespace", "example-id","example-data")
#  p r.get("example-namespace",offset)
class Rochefort
  # @param url - the rochefort url (String)
  def initialize(url)
    @urlSet = URI::join(url,'append').to_s
    @urlGet = URI::join(url,'get').to_s
    @urlScan = URI::join(url,'scan').to_s
    @urlGetMulti = URI::join(url,'getMulti').to_s
  end

  # append to rochefort, and returns the stored offset
  #  r = Rochefort.new(url)
  #  r.append(
  #        namespace: "ns",   # default nil (nil means the default namespace)
  #        data: "some data", # the data you want to append
  #        read_timeout: 1,   # default 1
  #        open_timeout: 1,   # default 1
  #  )
  # @return the offset at which the data was stored
  def append(opts)
    data = RestClient::Request.execute(method: :post,
                                       url: "#{@urlSet}?namespace=#{opts[:namespace]}&id=#{opts[:id]}",
                                       payload: opts[:data],
                                       read_timeout: opts[:read_timeout] || 1,
                                       open_timeout: opts[:open_timeout] || 1)
    out = JSON.parse(data.body)
    return out["offset"]
  end

  # get data from rochefort
  #  r = Rochefort.new(url)
  #  r.get(
  #        namespace: "ns", # default nil (nil means the default namespace)
  #        offset: 0,       # the offset returned from append()
  #        read_timeout: 1, # default 1
  #        open_timeout: 1, # default 1
  #  )
  # @return the stored data (String)
  def get(opts)
    data = RestClient::Request.execute(method: :get,
                                       url: "#{@urlGet}?namespace=#{opts[:namespace]}&offset=#{opts[:offset]}",
                                       read_timeout: opts[:read_timeout] || 1,
                                       open_timeout: opts[:open_timeout] || 1).body
    return data
  end

  # get multiple items from rochefort, (@see #get) (@see #append)
  #  r = Rochefort.new(url)
  #  r.getMulti(
  #        namespace: "ns", # default nil (nil means the default namespace)
  #        offsets: [],     # array of offsets
  #        read_timeout: 1, # default 1
  #        open_timeout: 1, # default 1
  #  )
  # @return array of stored elements (array of strings)
  def getMulti(opts)
    data = RestClient::Request.execute(method: :get,
                                       url: "#{@urlGetMulti}?namespace=#{opts[:namespace]}",
                                       payload: opts[:offsets].pack("q<*"),
                                       read_timeout: opts[:read_timeout] || 1,
                                       open_timeout: opts[:open_timeout] || 1).body
    offset = 0
    out = []
    while offset != data.length
      len = data.unpack('l<')[0]
      out << data[offset + 4, len]
      offset += 4 + len
    end
    return out
  end

  # scans a namespace, reading from a stream, so the namespace can be very big
  #  r = Rochefort.new(url)
  #  r.scan(namespace: ns) do |len, offset, value|
  #    puts value
  #  end
  # @return calls the block for each item
  def scan(opts,&input_block)
    block = proc do |response|
      buffer = ""
      header_len = 12
      need = header_len

      waiting_for_header = true

      len = 0
      rochefort_offset = 0

      response.read_body do |chunk|
        buffer << chunk
        while buffer.length >= need

          if waiting_for_header
            h = buffer.unpack('l<q<')
            len = h[0]
            rochefort_offset = h[1]
            buffer = buffer[header_len, buffer.length - header_len]
            need = len
            waiting_for_header = false
          end

          if buffer.length >= need
            input_block.call(len, rochefort_offset, buffer[0, len])
            buffer = buffer[len, buffer.length - len]
            need = header_len
            waiting_for_header = true
          end
        end
      end
    end

    RestClient::Request.execute(method: :get,
                                url: "#{@urlScan}?namespace=#{opts[:namespace]}",
                                read_timeout: opts[:read_timeout] || 1,
                                open_timeout: opts[:open_timeout] || 1,
                                block_response: block)
  end
end
