require 'minitest/autorun'
require 'rochefort'

class RochefortTest < Minitest::Unit::TestCase
  def test_rochefort
    url = ENV["ROCHEFORT_TEST"]
    if url 
      [nil,"atext","exam,ple"].each do |bucket|
        1.upto(10) do |id|
          1000.upto(1010) do |suffix|
            r = Rochefort.new("http://localhost:8001")
            data = "asdasd #{suffix}"
            offset = r.append(storage_prefix: bucket,id: id,data: data)
            fetched = r.get(storage_prefix: bucket, offset: offset)
            
            assert_equal data,fetched
            many = r.getMulti(storage_prefix: bucket,offsets: [offset,offset,offset,offset])
            assert_equal many,[data,data,data,data]
          end
        end
      end
    end
  end
end
