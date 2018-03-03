package nl.prymr.rochefort;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.concurrent.*;

public class ClientTest extends TestCase {
  public static final String[] namespaces = new String[] {"", "some-very-long-name", "example"};
  public static Map<String, Map<Long, byte[]>> lookupAllOffsets = new ConcurrentHashMap<>();
  Client client;

  public ClientTest(String testName) {
    super(testName);
  }

  public static Test suite() {
    return new TestSuite(ClientTest.class);
  }

  @Override
  public void setUp() throws Exception {
    client = new Client(System.getenv("ROCHEFORT_TEST"));
    for (final String s : namespaces) {
      lookupAllOffsets.put(s, new ConcurrentHashMap<Long, byte[]>());
      client.scan(
          s,
          new Client.ScanConsumer() {
            @Override
            public int getReadTimeout() {
              return 5000;
            }

            @Override
            public void accept(byte[] buffer, int length, long offset) {
              byte[] tmp = Arrays.copyOf(buffer, length);
              lookupAllOffsets.get(s).put(offset, tmp);
            }
          });
    }
  }

  public void testSearch() throws Exception {
    client.append("search", new String[] {"jaz"}, "abc".getBytes());
    client.append("search", new String[] {"jaz"}, "zzz".getBytes());
    client.append("search", new String[] {"jaz"}, "zzz2".getBytes());

    final List<String> matching = new ArrayList<>();
    client.scan(
        "search",
        new String[] {"jaz"},
        new Client.ScanConsumer() {
          @Override
          public void accept(byte[] buffer, int length, long rochefortOffset) throws Exception {
            matching.add(new String(Arrays.copyOfRange(buffer, 0, length)));
          }
        });
    assertEquals(matching.get(0), "abc");
    assertEquals(matching.get(1), "zzz");

    Client.Stats s = client.stats("search");
    assertEquals(Long.valueOf(3), s.Tags.get("jaz"));
  }

  public void testModify() throws Exception {
    long offset = client.append(1024, "abc".getBytes());
    assertEquals(new String(client.get(offset)), "abc");

    client.modify(offset, 1, "xyz".getBytes());

    assertEquals(new String(client.get(offset)), "axyz");

    lookupAllOffsets.get("").put(offset, client.get(offset));
  }

  public void testApp() throws Exception {
    Random random = new Random(System.currentTimeMillis());

    for (int attempt = 0; attempt < 2; attempt++) {

      for (final String namespace : namespaces) {
        List<byte[]> everything = new ArrayList<>();
        List<Long> allOffsets = new ArrayList<>();

        for (String id : new String[] {"abc", "abcd", "abcdef"}) {
          List<Long> offsets = new ArrayList<Long>();
          List<byte[]> stored = new ArrayList<byte[]>();
          for (int size :
              new int[] {
                1, 1, 1, 1, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
                10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
                10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 1000, 1000,
                10000, 100000, 1000000
              }) {

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int i = 0; i < size; i++) {
              bos.write(random.nextInt());
            }
            bos.flush();

            byte[] data = bos.toByteArray();
            long offset = client.append(namespace, data);
            offsets.add(offset);
            stored.add(data);

            // make sure we never get the same offset twice
            assertNull(
                "we already have offset " + offset, lookupAllOffsets.get(namespace).get(offset));
            lookupAllOffsets.get(namespace).put(offset, data);

            byte[] fetchedData = client.get(namespace, offset);
            assertTrue(Arrays.equals(data, fetchedData));

            long[] loffsets = new long[offsets.size()];
            for (int i = 0; i < offsets.size(); i++) loffsets[i] = offsets.get(i);
            List<byte[]> fetched = client.getMulti(namespace, loffsets);

            assertFalse(stored.size() == 0);
            assertEquals(stored.size(), fetched.size());
            for (int i = 0; i < stored.size(); i++) {
              assertTrue(Arrays.equals(stored.get(i), fetched.get(i)));
            }
          }

          everything.addAll(stored);
          allOffsets.addAll(offsets);
        }

        long[] loffsets = new long[allOffsets.size()];
        for (int i = 0; i < allOffsets.size(); i++) loffsets[i] = allOffsets.get(i);
        List<byte[]> fetched = client.getMulti(namespace, loffsets);

        assertFalse(fetched.size() == 0);
        assertEquals(everything.size(), fetched.size());
        for (int i = 0; i < everything.size(); i++) {
          assertTrue(Arrays.equals(everything.get(i), fetched.get(i)));
        }
      }
    }
  }

  public void testManyAsync() throws Exception {
    int threadCount = 10;
    Callable<Long> task =
        new Callable<Long>() {
          @Override
          public Long call() {
            try {
              testApp();
              return 1L;
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };
    List<Callable<Long>> tasks = Collections.nCopies(threadCount, task);
    ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    List<Future<Long>> futures = executorService.invokeAll(tasks);
    long sum = 0;
    for (Future<Long> f : futures) {
      sum += f.get();
    }
    assertEquals(sum, threadCount);
  }

  @Override
  public void tearDown() throws Exception {
    for (final String namespace : namespaces) {
      client.scan(
          namespace,
          new Client.ScanConsumer() {
            @Override
            public void accept(byte[] buffer, int length, long offset) {
              assertNotNull(
                  "missing offset " + offset + " on namespace " + namespace,
                  lookupAllOffsets.get(namespace).get(offset));
              byte[] tmp = Arrays.copyOf(buffer, length);
              byte[] stored = lookupAllOffsets.get(namespace).get(offset);

              assertTrue(Arrays.equals(tmp, stored));
            }
          });
    }
  }
}
