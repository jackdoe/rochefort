require 'minitest/autorun'
require 'rochefort'
require 'securerandom'
class RochefortTest < Minitest::Unit::TestCase
  def test_empty
    url = ENV["ROCHEFORT_TEST"]
    if url
      r = Rochefort.new(url)
      offset = r.append(namespace: 'empty', alloc_size: 1024, data: '')
      fetched = r.get(namespace: 'empty', offset: offset)
      assert_equal fetched,''
    end
  end

  def test_search
    url = ENV["ROCHEFORT_TEST"]
    if url
      r = Rochefort.new(url)

      a = []
      b = []
      ns = 'search'
      a << r.append(namespace: ns, data: 'aaa', tags: ['a'])
      a << r.append(namespace: ns, data: 'aaa2', tags: ['a','b'])
      a << r.append(namespace: ns, data: 'aaa3', tags: ['a','b','c'])

      b << r.append(namespace: ns, data: 'bbb', tags: ['b'])
      b << r.append(namespace: ns, data: 'bbb2', tags: ['a','b'])
      b << r.append(namespace: ns, data: 'bbb3', tags: ['a','b','c'])


      aa = []
      bb = []
      ab = []

      r.search(query: {tag: 'a'}, namespace: ns) do |offset, v|
        aa << v
      end

      r.search(query: {tag: 'b'}, namespace: ns) do |offset, v|
        bb << v
      end

      r.search(query: {or: [{tag: 'a'}, {tag: 'b'}]}, namespace: ns) do |offset, v|
        ab << v
      end

      aabb = []
      r.search(query: {and: [{tag: 'a'}, {tag: 'b'}]}, namespace: ns) do |offset, v|
        aabb << v
      end

      
      assert_equal(aa, ['aaa','aaa2','aaa3','bbb2','bbb3'])
      assert_equal(aabb, ['aaa2','aaa3','bbb2','bbb3'])
      assert_equal(bb, ['aaa2','aaa3','bbb','bbb2','bbb3'])
      assert_equal(ab, ['aaa','aaa2','aaa3','bbb','bbb2','bbb3'])
    end
  end


  def test_rochefort
    url = ENV["ROCHEFORT_TEST"]
    r = Rochefort.new(url)
    if url
      [nil,"atext","exam,ple"].each do |ns|
        everything_so_far = {}
        r.scan(namespace: ns) do |offset, data|
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
            r.scan(namespace: ns, open_timeout: 10) do |soffset, v|
              if !everything_so_far[soffset]
                raise "expected ${soffset}"
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
