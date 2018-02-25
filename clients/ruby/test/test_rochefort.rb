require 'minitest/autorun'
require 'rochefort'
require 'securerandom'
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
          1.upto(10) do |suffix|
            data = SecureRandom.random_bytes(suffix * 100)
            offset = r.append(namespace: ns, alloc_size: data.length * 2, data: data)
            fetched = r.get(namespace: ns, offset: offset)

            assert_equal data,fetched
            many = r.getMulti(namespace: ns,offsets: [offset,offset,offset,offset])
            assert_equal many,[data,data,data,data]

            # test modification head
            r.modify(namespace: ns, offset: offset, position: 0, data: "abc")
            fetchedAfter = r.get(namespace: ns, offset: offset)
            fetched[0,3] = 'abc'
            assert_equal fetched,fetchedAfter


            # test modification tail
            r.modify(namespace: ns, offset: offset, position: data.length - 4, data: "abcd")
            fetchedAfter = r.get(namespace: ns, offset: offset)
            fetched[fetched.length - 4, 4] = 'abcd'
            assert_equal fetched,fetchedAfter

            # test modification outside
            r.modify(namespace: ns, offset: offset, position: -1, data: "abcde")
            fetchedAfter = r.get(namespace: ns, offset: offset)
            fetched << 'abcde'
            assert_equal fetched,fetchedAfter


            # test modification near end of allocSize
            r.modify(namespace: ns, offset: offset, position: (data.length * 2) - 3, data: "zxc")
            fetchedAfter = r.get(namespace: ns, offset: offset)
            fetched[fetched.length, (data.length * 2) - fetched.length - 3] = "\x00" * ((data.length * 2) - fetched.length - 3)
            fetched << 'zxc'
            assert_equal fetched,fetchedAfter


            everything_so_far[offset] = fetched

            matching = 0
            r.scan(namespace: ns, open_timeout: 10) do |len, offset, v|
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
