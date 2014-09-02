package com.neverwinterdp.scribengin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;

public class ZookeeperFixture extends Fixture {
  private String host;
  private int port;
  private Process proc;
  private String version;
  private static final String TEMPLATED_PROPERTIES_FILE = "servers/%s/resources/zookeeper.properties";
  private static final String KAFKA_RUN_CLASS_SH = "servers/%s/kafka-bin/bin/kafka-run-class.sh";
  private static final String JAVA_MAIN = "org.apache.zookeeper.server.quorum.QuorumPeerMain";
  private static final String WAIT_FOR_REGEX = ".*?in standalone mode.*?";

  public ZookeeperFixture(String version, String host, int port) throws IOException {
    super();
    this.host = host;
    this.port = port;
    this.version = version;
  }

  public void start() throws IOException {
    HashMap<String, String> context = new HashMap<String, String>();
    context.put("tmp_dir", tmpDir.getAbsolutePath());
    context.put("host", this.host);
    context.put("port", Integer.toString(this.port));

    this.renderConfig(
      String.format(TEMPLATED_PROPERTIES_FILE, this.version),
      this.tmpDir.getAbsolutePath() + "/zookeeper.properties",
      context
    );

    ProcessBuilder pb = new ProcessBuilder(
      String.format(KAFKA_RUN_CLASS_SH, this.version),  //"servers/0.8.1/kafka-bin/bin/kafka-run-class.sh",
      JAVA_MAIN,                                        // "org.apache.zookeeper.server.quorum.QuorumPeerMain",
      tmpDir.getAbsolutePath() + "/zookeeper.properties"
    );
    this.proc =  pb.start();

    //block until we see the regex string
    BufferedReader br = new BufferedReader(new InputStreamReader(this.proc.getInputStream()));
    String line = null;
    while ((line = br.readLine()) != null) {
      if (line.matches(WAIT_FOR_REGEX)) {
        break;
      }
    }
  }


  public void stop() throws IOException {
    // Destroy the running process
    this.proc.destroy();
    // clean up the tmp directory
    FileUtils.deleteDirectory(this.tmpDir);
  }
}
