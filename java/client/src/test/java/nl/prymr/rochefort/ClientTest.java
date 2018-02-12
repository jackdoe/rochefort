package nl.prymr.rochefort;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.concurrent.*;

public class ClientTest extends TestCase {
  public static final String[] prefixes = new String[] {"", "some-very-long-name", "example"};
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
    client = new Client(System.getenv("ROCHEFORT_TEST_HOST"));
    for (final String s : prefixes) {
      lookupAllOffsets.put(s, new ConcurrentHashMap<Long, byte[]>());
      client.scan(
          s,
          new Client.ScanConsumer() {
            @Override
            public void accept(byte[] buffer, int length, long offset) {
              byte[] tmp = Arrays.copyOf(buffer, length);
              lookupAllOffsets.get(s).put(offset, tmp);
            }
          });
    }
  }

  public void testApp() throws Exception {
    Random random = new Random(System.currentTimeMillis());

    for (int attempt = 0; attempt < 2; attempt++) {

      for (final String storagePrefix : prefixes) {

        List<byte[]> everything = new ArrayList<>();
        List<Long> allOffsets = new ArrayList<>();

        for (String id : new String[] {"abc", "abcd", "abcdef"}) {
          id = id + random.nextFloat();
          List<Long> offsets = new ArrayList<Long>();
          List<byte[]> stored = new ArrayList<byte[]>();
          for (int size :
              new int[] {
                1, 1, 1, 1, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
                10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
                10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
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
            long offset = client.append(storagePrefix, id, data);
            offsets.add(offset);
            stored.add(data);

            // make sure we never get the same offset twice
            assertNull("we already have offset " + offset, lookupAllOffsets.get(offset));
            lookupAllOffsets.get(storagePrefix).put(offset, data);

            byte[] fetchedData = client.get(storagePrefix, offset);
            //            assertTrue(
            //                String.format(
            //                    "storagePrefix:%s id:%s size: %d offset: %d expected: %s got %s",
            //                    storagePrefix,
            //                    id,
            //                    size,
            //                    offset,
            //                    Arrays.toString(data),
            //                    Arrays.toString(fetchedData)),
            //                Arrays.equals(data, fetchedData));
            assertTrue(Arrays.equals(data, fetchedData));

            long[] loffsets = new long[offsets.size()];
            for (int i = 0; i < offsets.size(); i++) loffsets[i] = offsets.get(i);
            List<byte[]> fetched = client.getMulti(storagePrefix, loffsets);

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
        List<byte[]> fetched = client.getMulti(storagePrefix, loffsets);

        assertFalse(fetched.size() == 0);
        assertEquals(everything.size(), fetched.size());
        for (int i = 0; i < everything.size(); i++) {
          assertTrue(Arrays.equals(everything.get(i), fetched.get(i)));
        }
      }

      synchronized (ClientTest.class) {
        // no insert while we are scanning, because otherwise we cant verify everything is 100% in
        // place
        for (final String storagePrefix : prefixes) {
          client.scan(
              storagePrefix,
              new Client.ScanConsumer() {
                @Override
                public void accept(byte[] buffer, int length, long offset) {
                  assertNotNull(
                      "missing offset " + offset, lookupAllOffsets.get(storagePrefix).get(offset));
                  byte[] tmp = Arrays.copyOf(buffer, length);
                  byte[] stored = lookupAllOffsets.get(storagePrefix).get(offset);
                  //                  assertTrue(
                  //                      String.format(
                  //                          "offset: %d expected: %s got %s",
                  //                          offset, Arrays.toString(tmp),Arrays.toString(stored)),
                  //                      Arrays.equals(tmp, stored));
                  assertTrue(Arrays.equals(tmp, stored));
                }
              });
        }
      }
    }
  }

  public void testManyAsync() throws Exception {
    int threadCount = 20;
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
}
