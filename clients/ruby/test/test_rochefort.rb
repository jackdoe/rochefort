require 'minitest/autorun'
require 'rochefort'

class RochefortTest < Minitest::Unit::TestCase
  def test_rochefort
    url = ENV["ROCHEFORT_TEST"]
    if url 
      [nil,"atext","exam,ple"].each do |ns|
        1.upto(10) do |id|
          1000.upto(1010) do |suffix|
            r = Rochefort.new(url)
            data = "asdasd #{suffix}"
            offset = r.append(namespace: ns,id: id,data: data)
            fetched = r.get(namespace: ns, offset: offset)
            
            assert_equal data,fetched
            many = r.getMulti(namespace: ns,offsets: [offset,offset,offset,offset])
            assert_equal many,[data,data,data,data]
          end
        end
      end
    end
  end
end
