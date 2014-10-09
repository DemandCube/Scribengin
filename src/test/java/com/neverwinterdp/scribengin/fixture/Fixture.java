package com.neverwinterdp.scribengin.fixture;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public abstract class Fixture {
  protected String version;
  protected String host;
  protected int port;
  protected static final String KAFKA_RUN_CLASS_SH = "servers/%s/kafka-bin/bin/kafka-run-class.sh";
  protected File tmpDir;

  public Fixture() throws IOException {
    this.tmpDir = this.mktemp();
  }

  protected File mktemp() throws IOException {
    File tmp = File.createTempFile(Long.toString(System.nanoTime()), "");
    tmp.delete();
    tmp.mkdir();
    return tmp;
  }


  protected void renderConfig(String src, String dst, HashMap<String, String> context)
      throws IOException {
    // 1. read in the templated config file
    // 2. replace all the templated variables with values in context.
    // 3. write it back out the dst file
    BufferedReader br = new BufferedReader(new FileReader(src));
    StringBuilder sb = new StringBuilder();
    String line = null;
    while ((line = br.readLine()) != null) {
      sb.append(line);
      sb.append("\n");
    }
    String content = sb.toString();

    for (Map.Entry<String, String> entry : context.entrySet()) {
      String k = String.format("{%s}", entry.getKey());
      String v = entry.getValue();
      content = content.replace(k, v);
    }

    PrintWriter out = new PrintWriter(dst);
    out.write(content);

    out.close();
    br.close();
  }

  public String getHost() {
    return this.host;
  }

  public int getPort() {
    return this.port;
  }

  abstract public void start() throws IOException;

  abstract public void stop() throws IOException;

  abstract public void install() throws IOException;
}
