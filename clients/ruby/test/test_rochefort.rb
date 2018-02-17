require 'minitest/autorun'
require 'rochefort'

class RochefortTest < Minitest::Unit::TestCase
  def test_rochefort
    url = ENV["ROCHEFORT_TEST"]
    r = Rochefort.new(url)
    if url
      [nil,"atext","exam,ple"].each do |ns|
        everything_so_far = {}
        r.scan(namespace: ns) do |len, offset, data|
          everything_so_far[offset] = data
        end

        1.upto(10) do |id|
          800.upto(1010) do |suffix|
            data = "asdasd #{suffix} #{rand(36**suffix).to_s(36)}"
            offset = r.append(namespace: ns,id: id,data: data)
            fetched = r.get(namespace: ns, offset: offset)

            assert_equal data,fetched
            many = r.getMulti(namespace: ns,offsets: [offset,offset,offset,offset])
            assert_equal many,[data,data,data,data]

            everything_so_far[offset] = fetched

            matching = 0
            r.scan(namespace: ns) do |len, offset, v|
              if !everything_so_far[offset]
                raise "expected ${offset}"
              end
              matching += 1
            end

            assert_equal matching, everything_so_far.length
          end
        end
      end
    end
  end
end
