package nl.prymr.rochefort;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static nl.prymr.rochefort.Util.convertStreamToString;
import static nl.prymr.rochefort.Util.readFully;

public class Client {
  private String urlGetMulti, urlGet, urlAppend, urlScan;

  public Client(String url) throws Exception {
    this(new URL(url));
  }

  public Client(URL url) {
    String prefix;

    if (url.getPort() != -1) {
      prefix = url.getProtocol() + "://" + url.getHost() + ":" + url.getPort();
    } else {
      prefix = url.getProtocol() + "://" + url.getHost();
    }
    prefix += url.getPath();
    if (!url.getPath().endsWith("/")) {
      prefix += "/";
    }

    this.urlGet = prefix + "get";
    this.urlGetMulti = prefix + "getMulti";
    this.urlAppend = prefix + "append";
    this.urlScan = prefix + "scan";
  }

  public static long append(String urlSet, String storagePrefix, String id, byte[] data)
      throws Exception {
    HttpResponse<InputStream> response =
        Unirest.post(urlSet)
            .queryString("id", id)
            .queryString("storagePrefix", storagePrefix)
            .body(data)
            .asBinary();
    if (response.getStatus() != 200) {
      throw new Exception(
          "status code "
              + response.getStatus()
              + " url: "
              + urlSet
              + " body: "
              + convertStreamToString(response.getRawBody()));
    }
    byte[] ret = readFully(response.getBody(), -1, true);

    return new JsonNode(new String(ret)).getObject().getLong("offset");
  }

  public static List<byte[]> getMulti(
      String urlGetMulti, String storagePrefix, long[] listOfOffsets) throws Exception {
    return getMulti(urlGetMulti, storagePrefix, Util.listOfLongsToBytes(listOfOffsets));
  }

  public static byte[] get(String urlGet, String storagePrefix, long offset) throws Exception {
    HttpResponse<InputStream> response =
        Unirest.get(urlGet)
            .queryString("storagePrefix", storagePrefix)
            .queryString("offset", offset)
            .asBinary();
    if (response.getStatus() != 200) {
      throw new Exception(
          "status code "
              + response.getStatus()
              + " url: "
              + urlGet
              + " storagePrefix: "
              + storagePrefix
              + " body: "
              + convertStreamToString(response.getRawBody()));
    }

    return readFully(response.getRawBody(), -1, true);
  }

  public static List<byte[]> getMulti(
      String urlGetMulti, String storagePrefix, byte[] encodedListOfOffsets) throws Exception {

    HttpResponse<InputStream> response =
        Unirest.post(urlGetMulti)
            .queryString("storagePrefix", storagePrefix)
            .body(encodedListOfOffsets)
            .asBinary();
    if (response.getStatus() != 200) {
      throw new Exception(
          "status code "
              + response.getStatus()
              + " url: "
              + urlGetMulti
              + "storagePrefix: "
              + storagePrefix
              + " body: "
              + convertStreamToString(response.getRawBody()));
    }

    List<byte[]> out = new ArrayList<byte[]>(encodedListOfOffsets.length / 8);

    byte[] data = readFully(response.getRawBody(), -1, true);
    int offset = 0;

    while (true) {
      if (data.length < offset + 4) return out;

      int len = Util.aByteToInt(data, offset);
      offset += 4;
      if (len == 0) {
        throw new Exception("read errror url: " + urlGetMulti);
      }

      byte[] stored = new byte[len];
      System.arraycopy(data, offset, stored, 0, len);
      out.add(stored);

      offset += len;
      if (offset == data.length) break;
    }
    return out;
  }

  public static void scan(String urlGetScan, String storagePrefix, ScanConsumer consumer)
      throws Exception {

    HttpResponse<InputStream> response =
        Unirest.get(urlGetScan).queryString("storagePrefix", storagePrefix).asBinary();

    if (response.getStatus() != 200) {
      throw new Exception(
          "status code "
              + response.getStatus()
              + " url: "
              + urlGetScan
              + "storagePrefix: "
              + storagePrefix
              + " body: "
              + convertStreamToString(response.getRawBody()));
    }
    byte[] buffer = new byte[65535];
    byte[] header = new byte[12];
    DataInputStream is = new DataInputStream(response.getRawBody());

    while (true) {
      try {
        is.readFully(header, 0, header.length);
      } catch (EOFException e) {
        break;
      }
      int len = Util.aByteToInt(header, 0);
      long offset = Util.abyteToLong(header, 4);
      if (buffer.length < len) {
        buffer = new byte[len * 2];
      }
      is.readFully(buffer, 0, len);
      consumer.accept(buffer, len, offset);
    }
  }

  public long append(String id, byte[] data) throws Exception {
    return append("", id, data);
  }

  public long append(String storagePrefix, String id, byte[] data) throws Exception {
    return append(this.urlAppend, storagePrefix, id, data);
  }

  public byte[] get(long offset) throws Exception {
    return get("", offset);
  }

  public byte[] get(String storagePrefix, long offset) throws Exception {
    return get(this.urlGet, storagePrefix, offset);
  }

  public List<byte[]> getMulti(long[] listOfOffsets) throws Exception {
    return getMulti("", listOfOffsets);
  }

  public List<byte[]> getMulti(byte[] encodedListOfOffsets) throws Exception {
    return getMulti("", encodedListOfOffsets);
  }

  public List<byte[]> getMulti(String storagePrefix, long[] listOfOffsets) throws Exception {
    return getMulti(this.urlGetMulti, storagePrefix, listOfOffsets);
  }

  public List<byte[]> getMulti(String storagePrefix, byte[] encodedListOfOffsets) throws Exception {
    return getMulti(this.urlGetMulti, storagePrefix, encodedListOfOffsets);
  }

  public void scan(ScanConsumer consumer) throws Exception {
    scan(this.urlScan, "", consumer);
  }

  public void scan(String storagePrefix, ScanConsumer consumer) throws Exception {
    scan(this.urlScan, storagePrefix, consumer);
  }

  public abstract static class ScanConsumer {
    public abstract void accept(byte[] buffer, int length, long rochefortOffset);
  }
}
