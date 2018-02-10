package nl.prymr.rochefort;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class Util {
  public static byte[] listOfLongsToBytes(long[] longs) {
    byte[] bytes = new byte[longs.length * 8];
    for (int i = 0, j = 0; i < longs.length; i++, j += 8) {
      bytes[j + 7] = (byte) (longs[i] >> 56);
      bytes[j + 6] = (byte) (longs[i] >> 48);
      bytes[j + 5] = (byte) (longs[i] >> 40);
      bytes[j + 4] = (byte) (longs[i] >> 32);
      bytes[j + 3] = (byte) (longs[i] >> 24);
      bytes[j + 2] = (byte) (longs[i] >> 16);
      bytes[j + 1] = (byte) (longs[i] >> 8);
      bytes[j + 0] = (byte) (longs[i]);
    }

    return bytes;
  }

  public static long abyteToLong(byte[] bytes, int off) {
    long r =
        ((long) (bytes[off + 7] & 0xFF)) << 56L
            | ((long) (bytes[off + 6] & 0xFF)) << 48L
            | ((long) (bytes[off + 5] & 0xFF)) << 40L
            | ((long) (bytes[off + 4] & 0xFF)) << 32L
            | ((long) (bytes[off + 3] & 0xFF)) << 24L
            | ((long) (bytes[off + 2] & 0xFF)) << 16L
            | ((long) (bytes[off + 1] & 0xFF)) << 8L
            | ((long) (bytes[off + 0] & 0xFF));
    return r;
  }

  public static long[] listOfBytesToLongs(byte[] bytes) {
    long[] longs = new long[bytes.length / 8];
    for (int i = 0, j = 0; i < bytes.length; i += 8, j++) {
      longs[j] = abyteToLong(bytes, i);
    }
    return longs;
  }

  public static long[] listOfBytesToLongsPlusExtraOne(byte[] bytes) {
    long[] longs = new long[(bytes.length / 8) + 1];
    for (int i = 0, j = 0; i < bytes.length; i += 8, j++) {
      longs[j] = abyteToLong(bytes, i);
    }
    return longs;
  }

  public static int aByteToInt(byte[] bytes, int off) {
    int r =
        ((int) ((int) (bytes[off + 3] & 0xFF)) << 24L
            | ((int) (bytes[off + 2] & 0xFF)) << 16L
            | ((int) (bytes[off + 1] & 0xFF)) << 8L
            | ((int) (bytes[off + 0] & 0xFF)));

    return r;
  }

  public static String convertStreamToString(java.io.InputStream is) {
    java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
    return s.hasNext() ? s.next() : "";
  }

  public static byte[] readFully(InputStream is, int length, boolean readAll) throws IOException {
    byte[] output = {};
    if (length == -1) length = Integer.MAX_VALUE;
    int pos = 0;
    while (pos < length) {
      int bytesToRead;
      if (pos >= output.length) { // Only expand when there's no room
        bytesToRead = Math.min(length - pos, output.length + 1024);
        if (output.length < pos + bytesToRead) {
          output = Arrays.copyOf(output, pos + bytesToRead);
        }
      } else {
        bytesToRead = output.length - pos;
      }
      int cc = is.read(output, pos, bytesToRead);
      if (cc < 0) {
        if (readAll && length != Integer.MAX_VALUE) {
          throw new EOFException("Detect premature EOF");
        } else {
          if (output.length != pos) {
            output = Arrays.copyOf(output, pos);
          }
          break;
        }
      }
      pos += cc;
    }
    return output;
  }
}
