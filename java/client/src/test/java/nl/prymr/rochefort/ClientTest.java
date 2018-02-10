package nl.prymr.rochefort;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class ClientTest extends TestCase {
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
  }

  public void testApp() throws Exception {
    Random random = new Random(System.currentTimeMillis());
    for (int attempt = 0; attempt < 5; attempt++) {
      for (String storagePrefix : new String[] {"", "some-very-long-name", "example"}) {
        for (String id : new String[] {"abc", "abcd", "abcdef"}) {
          id = id + random.nextFloat();
          List<Long> offsets = new ArrayList<Long>();
          List<byte[]> stored = new ArrayList<byte[]>();

          for (int size :
              new int[] {
                1, 1, 1, 1, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
                10, 10, 1000, 1000, 10000, 100000, 1000000
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

            byte[] fetchedData = client.get(storagePrefix, id, offset);
            assertTrue(
                String.format(
                    "storagePrefix:%s id:%s size: %d offset: %d expected: %s got %s",
                    storagePrefix,
                    id,
                    size,
                    offset,
                    Arrays.toString(data),
                    Arrays.toString(fetchedData)),
                Arrays.equals(data, fetchedData));

            long[] loffsets = new long[offsets.size()];
            for (int i = 0; i < offsets.size(); i++) loffsets[i] = offsets.get(i);
            List<byte[]> fetched = client.getMulti(storagePrefix, id, loffsets);

            assertEquals(stored.size(), fetched.size());
            for (int i = 0; i < stored.size(); i++) {
              assertTrue(Arrays.equals(stored.get(i), fetched.get(i)));
            }
          }
        }
      }
    }
  }
}
