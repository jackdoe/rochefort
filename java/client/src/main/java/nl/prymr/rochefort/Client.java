package nl.prymr.rochefort;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static nl.prymr.rochefort.Util.convertStreamToString;
import static nl.prymr.rochefort.Util.readFully;

public class Client {
  private String urlGetMulti, urlGet, urlAppend;

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
      String urlGetMulti, String storagePrefix, String id, long[] listOfOffsets) throws Exception {
    return getMulti(urlGetMulti, storagePrefix, id, Util.listOfLongsToBytes(listOfOffsets));
  }

  public static byte[] get(String urlGet, String storagePrefix, String id, long offset)
      throws Exception {
    HttpResponse<InputStream> response =
        Unirest.get(urlGet)
            .queryString("storagePrefix", storagePrefix)
            .queryString("id", id)
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
      String urlGetMulti, String storagePrefix, String id, byte[] encodedListOfOffsets)
      throws Exception {

    HttpResponse<InputStream> response =
        Unirest.post(urlGetMulti)
            .queryString("id", id)
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

  public long append(String id, byte[] data) throws Exception {
    return append("", id, data);
  }

  public long append(String storagePrefix, String id, byte[] data) throws Exception {
    return append(this.urlAppend, storagePrefix, id, data);
  }

  public byte[] get(String id, long offset) throws Exception {
    return get("", id, offset);
  }

  public byte[] get(String storagePrefix, String id, long offset) throws Exception {
    return get(this.urlGet, storagePrefix, id, offset);
  }

  public List<byte[]> getMulti(String id, long[] listOfOffsets) throws Exception {
    return getMulti("", id, listOfOffsets);
  }

  public List<byte[]> getMulti(String id, byte[] encodedListOfOffsets) throws Exception {
    return getMulti("", id, encodedListOfOffsets);
  }

  public List<byte[]> getMulti(String storagePrefix, String id, long[] listOfOffsets)
      throws Exception {
    return getMulti(this.urlGetMulti, storagePrefix, id, listOfOffsets);
  }

  public List<byte[]> getMulti(String storagePrefix, String id, byte[] encodedListOfOffsets)
      throws Exception {
    return getMulti(this.urlGetMulti, storagePrefix, id, encodedListOfOffsets);
  }
}