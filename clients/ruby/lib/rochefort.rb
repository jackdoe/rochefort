require 'rest-client'
require 'uri'
require 'json'

# example usage
# r = new Rochefort("http://localhost:8001")
# offset = r.append("example-namespace", "example-id","example-data")
# p r.get("example-namespace",offset)



class Rochefort
  def initialize(url)
    @urlSet = URI::join(url,'append').to_s
    @urlGet = URI::join(url,'get').to_s
    @urlGetMulti = URI::join(url,'getMulti').to_s
  end

  def append(opts)
    data = RestClient::Request.execute(method: :post, url: "#{@urlSet}?storagePrefix=#{opts[:storage_prefix]}&id=#{opts[:id]}",payload: opts[:data], read_timeout: opts[:read_timeout] || 1, open_timeout: opts[:open_timeout] || 1)
    out = JSON.parse(data.body)
    return out["offset"]
  end

  def get(opts)
    data = RestClient::Request.execute(method: :get, url: "#{@urlGet}?storagePrefix=#{opts[:storage_prefix]}&offset=#{opts[:offset]}", read_timeout: opts[:read_timeout] || 1, open_timeout: opts[:open_timeout] || 1)
    return data
  end 

  def getMulti(opts)
    data = RestClient::Request.execute(method: :get, url: "#{@urlGetMulti}?storagePrefix=#{opts[:storage_prefix]}", payload: opts[:offsets].pack("q<*"), read_timeout: opts[:read_timeout] || 1, open_timeout: opts[:open_timeout] || 1).body
    offset = 0
    out = []
    while offset != data.length
      len = data.unpack('l<')[0]
      out << data.slice(offset + 4, len)
      offset += 4 + len
    end
    return out
  end
end
