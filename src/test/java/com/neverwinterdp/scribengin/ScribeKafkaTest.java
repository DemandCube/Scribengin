package com.neverwinterdp.scribengin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class ScribeKafkaTest {
  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    System.out.println("calling setup..."); //xxx
    Process p = Runtime.getRuntime().exec("script/bootstrap_kafka.sh servers");
    p.waitFor();
    BufferedReader reader =
      new BufferedReader(new InputStreamReader(p.getInputStream()));

    String line = "";
    while ((line = reader.readLine())!= null) {
      System.out.println(line);//xxx
    }
  }


  @AfterClass
  public static void teardown() {
    System.out.println("calling teardown.."); //xxx
  }

  protected File mktemp() throws IOException {
    File tmp = File.createTempFile(Long.toString(System.nanoTime()), "");
    tmp.delete();
    tmp.mkdir();
    return tmp;
  }

  protected void renderConfig(String src, String dst, HashMap<String, String> context) throws IOException {
    // 1. read in the templated config file
    // 2. replace all the templated variables with values in context.
    // 3. write it back out the dst file
    BufferedReader br = new BufferedReader(new FileReader(src));
    StringBuilder sb = new StringBuilder();
    String line = null;
    while((line = br.readLine()) != null) {
      sb.append(line);
      sb.append("\n");
    }
    String content = sb.toString();

    for(Map.Entry<String, String> entry : context.entrySet()) {
      String k = String.format("{%s}", entry.getKey());
      String v = entry.getValue();
      content = content.replace(k, v);
    }

    PrintWriter out = new PrintWriter(dst);
    out.write(content);

    out.close();
    br.close();
  }

  protected Process startZookeeper() throws IOException {
    // create a tmp location
    File tmpDir = mktemp();

    // render config
    HashMap<String, String> context = new HashMap<String, String>();
    context.put("tmp_dir", tmpDir.getAbsolutePath());
    context.put("host", "127.0.0.1");
    context.put("port", "2111");

    renderConfig(
      "servers/0.8.1/resources/zookeeper.properties",
      tmpDir.getAbsolutePath() + "/zookeeper.properties",
      context
    );

    ProcessBuilder pb = new ProcessBuilder(
      "servers/0.8.1/kafka-bin/bin/kafka-run-class.sh",
      "org.apache.zookeeper.server.quorum.QuorumPeerMain",
      tmpDir.getAbsolutePath() + "/zookeeper.properties"
    );
    //pb.redirectErrorStream();
    Process p = pb.start();
    return p;
  }


  @Test
  public void testKafka() throws IOException {
    try {
      Process p = startZookeeper();
      BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line;
      while ((line = br.readLine()) != null) {
        System.out.println(line);
        if (line.matches(".*?in standalone mode.*?")) {
          break;
        }
      }

      System.out.println("here...");//xxx
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
