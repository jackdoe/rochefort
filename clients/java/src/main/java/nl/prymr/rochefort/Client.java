package nl.prymr.rochefort;

import com.google.protobuf.ByteString;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static nl.prymr.rochefort.Util.convertStreamToString;

public class Client {
  private String urlGet, urlSet, urlScan, urlStats, urlQuery, urlDelete;

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
    this.urlSet = prefix + "set";
    this.urlScan = prefix + "scan";
    this.urlQuery = prefix + "query";
    this.urlStats = prefix + "stat";
    this.urlDelete = prefix + "delete";
  }

  public static long append(
      String urlSet, String namespace, String[] tags, int allocSize, byte[] data) throws Exception {
    Proto.Append appendPayload =
        Proto.Append.newBuilder()
            .addAllTags(Arrays.asList(tags == null ? new String[0] : tags))
            .setData(ByteString.copyFrom(data))
            .setNamespace(namespace)
            .setAllocSize(allocSize)
            .build();
    Proto.AppendInput input =
        Proto.AppendInput.newBuilder().addAppendPayload(appendPayload).build();

    HttpResponse<InputStream> response = Unirest.post(urlSet).body(input.toByteArray()).asBinary();
    if (response.getStatus() != 200) {
      throw new Exception(
          "status code "
              + response.getStatus()
              + " url: "
              + urlSet
              + " body: "
              + convertStreamToString(response.getRawBody()));
    }

    Proto.AppendOutput out = Proto.AppendOutput.parseFrom(response.getBody());
    return out.getOffset(0);
  }

  public static boolean modify(
      String urlSet, String namespace, long offset, int position, boolean resetLength, byte[] data)
      throws Exception {
    Proto.Modify modifyPayload =
        Proto.Modify.newBuilder()
            .setData(ByteString.copyFrom(data))
            .setNamespace(namespace)
            .setOffset(offset)
            .setPos(position)
            .setResetLength(resetLength)
            .build();
    Proto.AppendInput input =
        Proto.AppendInput.newBuilder().addModifyPayload(modifyPayload).build();

    HttpResponse<InputStream> response = Unirest.post(urlSet).body(input.toByteArray()).asBinary();
    if (response.getStatus() != 200) {
      throw new Exception(
          "status code "
              + response.getStatus()
              + " url: "
              + urlSet
              + " body: "
              + convertStreamToString(response.getRawBody()));
    }

    Proto.AppendOutput out = Proto.AppendOutput.parseFrom(response.getBody());
    return out.getModifiedCount() > 0;
  }

  public static byte[] get(String urlGet, String namespace, long offset) throws Exception {
    long[] offsets = new long[1];
    offsets[0] = offset;
    return get(urlGet, namespace, offsets).get(0);
  }

  public static List<byte[]> get(String urlGet, String namespace, long[] offsets) throws Exception {
    List<Proto.Get> payloads = new ArrayList<>(offsets.length);
    for (long offset : offsets) {
      payloads.add(Proto.Get.newBuilder().setNamespace(namespace).setOffset(offset).build());
    }
    Proto.GetInput input = Proto.GetInput.newBuilder().addAllGetPayload(payloads).build();
    HttpResponse<InputStream> response = Unirest.post(urlGet).body(input.toByteArray()).asBinary();
    if (response.getStatus() != 200) {
      throw new Exception(
          "status code "
              + response.getStatus()
              + " url: "
              + urlGet
              + " body: "
              + convertStreamToString(response.getRawBody()));
    }

    List<byte[]> out = new ArrayList<>();
    Proto.GetOutput ret = Proto.GetOutput.parseFrom(response.getBody());
    for (ByteString b : ret.getDataList()) {
      out.add(b.toByteArray());
    }
    return out;
  }

  public static boolean delete(String urlDelete, String namespace) throws Exception {
    Proto.NamespaceInput input = Proto.NamespaceInput.newBuilder().setNamespace(namespace).build();

    HttpResponse<InputStream> response =
        Unirest.post(urlDelete).body(input.toByteArray()).asBinary();
    if (response.getStatus() != 200) {
      throw new Exception(
          "status code "
              + response.getStatus()
              + " url: "
              + urlDelete
              + " namespace: "
              + namespace
              + " body: "
              + convertStreamToString(response.getRawBody()));
    }

    Proto.SuccessOutput out = Proto.SuccessOutput.parseFrom(response.getBody());
    return out.getSuccess();
  }

  public static void scan(String urlQuery, String namespace, Query query, ScanConsumer consumer)
      throws Exception {
    URL url = new URL((urlQuery + "?namespace=" + namespace));

    // XXX: Unirest reads the whole body, which makes the scan useless
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();

    connection.setRequestMethod("POST");
    connection.setReadTimeout(consumer.getReadTimeout());
    connection.setConnectTimeout(consumer.getConnectTimeout());
    connection.setRequestProperty("Connection", "close");
    InputStream inputStream = null;
    InputStreamReader reader = null;

    try {
      if (query != null) {
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json");
        JSONObject json = new JSONObject(query);
        OutputStream os = connection.getOutputStream();
        OutputStreamWriter writer = new OutputStreamWriter(os);
        json.write(writer);
        writer.flush();
        writer.close();
        os.close();
      } else {
        connection.setDoOutput(true);
      }

      inputStream = connection.getInputStream();
      byte[] buffer = new byte[65535];
      byte[] header = new byte[12];
      DataInputStream is = new DataInputStream(inputStream);

      while (true) {
        try {
          is.readFully(header, 0, header.length);
        } catch (EOFException e) {
          break;
        }
        int len = Util.aByteToInt(header, 0);
        long offset = Util.abyteToLong(header, 4);
        if (buffer.length < len) {
          buffer = new byte[len];
        }
        is.readFully(buffer, 0, len);
        consumer.accept(buffer, len, offset);
      }

    } catch (Exception e) {
      int code = connection.getResponseCode();
      throw new Exception(
          "status code "
              + code
              + " url: "
              + urlQuery
              + " namespace: "
              + namespace
              + " exception: "
              + e.getMessage());
    } finally {
      if (reader != null) {
        reader.close();
      }
      if (inputStream != null) {
        inputStream.close();
      }
      connection.disconnect();
    }
  }

  public static Proto.StatsOutput stats(String urlStats, String namespace) throws Exception {
    Proto.NamespaceInput input = Proto.NamespaceInput.newBuilder().setNamespace(namespace).build();

    HttpResponse<InputStream> response =
        Unirest.post(urlStats).body(input.toByteArray()).asBinary();
    if (response.getStatus() != 200) {
      throw new Exception(
          "status code "
              + response.getStatus()
              + " url: "
              + urlStats
              + " namespace: "
              + namespace
              + " body: "
              + convertStreamToString(response.getRawBody()));
    }

    Proto.StatsOutput out = Proto.StatsOutput.parseFrom(response.getBody());
    return out;
  }

  public boolean modify(long offset, int position, boolean resetLength, byte[] data)
      throws Exception {
    return modify("", offset, position, resetLength, data);
  }

  public boolean modify(
      String namespace, long offset, int position, boolean resetLength, byte[] data)
      throws Exception {
    return modify(this.urlSet, namespace, offset, position, resetLength, data);
  }

  public long append(byte[] data) throws Exception {
    return append("", null, 0, data);
  }

  public long append(String[] tags, byte[] data) throws Exception {
    return append("", tags, 0, data);
  }

  public long append(int allocSize, byte[] data) throws Exception {
    return append("", null, allocSize, data);
  }

  public long append(String[] tags, int allocSize, byte[] data) throws Exception {
    return append("", tags, allocSize, data);
  }

  public long append(String namespace, byte[] data) throws Exception {
    return append(namespace, null, 0, data);
  }

  public long append(String namespace, String[] tags, byte[] data) throws Exception {
    return append(namespace, tags, 0, data);
  }

  public long append(String namespace, String[] tags, int allocSize, byte[] data) throws Exception {
    return append(this.urlSet, namespace, tags, allocSize, data);
  }

  public byte[] get(long offset) throws Exception {
    return get("", offset);
  }

  public byte[] get(String namespace, long offset) throws Exception {
    return get(this.urlGet, namespace, offset);
  }

  public List<byte[]> getMulti(long[] listOfOffsets) throws Exception {
    return getMulti("", listOfOffsets);
  }

  public List<byte[]> getMulti(byte[] encodedListOfOffsets) throws Exception {
    return getMulti("", encodedListOfOffsets);
  }

  public List<byte[]> getMulti(String namespace, long[] listOfOffsets) throws Exception {
    return get(this.urlGet, namespace, listOfOffsets);
  }

  public List<byte[]> getMulti(String namespace, byte[] encodedListOfOffsets) throws Exception {
    return get(this.urlGet, namespace, Util.listOfBytesToLongs(encodedListOfOffsets));
  }

  public void scan(ScanConsumer consumer) throws Exception {
    scan(this.urlScan, "", null, consumer);
  }

  public void scan(String namespace, ScanConsumer consumer) throws Exception {
    scan(this.urlScan, namespace, null, consumer);
  }

  public void search(Query query, ScanConsumer consumer) throws Exception {
    scan(this.urlQuery, "", query, consumer);
  }

  public void search(String namespace, Query query, ScanConsumer consumer) throws Exception {
    scan(this.urlQuery, namespace, query, consumer);
  }

  public Proto.StatsOutput stats(String namespace) throws Exception {
    return stats(this.urlStats, namespace);
  }

  public boolean delete(String namespace) throws Exception {
    return delete(this.urlDelete, namespace);
  }

  public abstract static class ScanConsumer {
    public int getConnectTimeout() {
      return 1000;
    }

    public int getReadTimeout() {
      return 1000;
    }

    public abstract void accept(byte[] buffer, int length, long rochefortOffset) throws Exception;
  }

  public static final class Query {
    public List<Query> and;
    public List<Query> or;
    public String tag;

    public Query(String t) {
      this.tag = t;
    }

    public Query() {}

    public String getTag() {
      return tag;
    }

    public List<Query> getAnd() {
      return and;
    }

    public List<Query> getOr() {
      return or;
    }

    public Query and(Query... qq) {
      if (this.and == null) this.and = new ArrayList<>();

      for (Query q : qq) this.and.add(q);
      return this;
    }

    public Query or(Query... qq) {
      if (this.or == null) this.or = new ArrayList<>();

      for (Query q : qq) this.or.add(q);
      return this;
    }
  }
}
