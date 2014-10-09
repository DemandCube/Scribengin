package com.neverwinterdp.scribengin.fixture;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

public class ZookeeperFixture extends Fixture {
  private Process proc;
  private static final String PROPERTIES_FILENAME = "/zookeeper.properties";
  private static final String LOG4J_FILENAME = "/log4j.properties";
  private static final String TEMPLATED_LOG4J_FULLPATH = "servers/%s/resources" + LOG4J_FILENAME;

  private static final String TEMPLATED_PROPERTIES_FULLPATH = "servers/%s/resources"
      + PROPERTIES_FILENAME;
  private static final String JAVA_MAIN = "org.apache.zookeeper.server.quorum.QuorumPeerMain";
  private static final String WAIT_FOR_REGEX = ".*?Established session.*?";

  public ZookeeperFixture(String version, String host, int port) throws IOException {
    super();
    this.host = host;
    this.port = port;
    this.version = version;
  }

  @Override
  public void start() throws IOException {
    HashMap<String, String> context = new HashMap<String, String>();
    context.put("tmp_dir", tmpDir.getAbsolutePath());
    context.put("host", this.host);
    context.put("port", Integer.toString(this.port));

    this.renderConfig(
        String.format(TEMPLATED_PROPERTIES_FULLPATH, this.version),
        this.tmpDir.getAbsolutePath() + PROPERTIES_FILENAME,
        context);

    this.renderConfig(
        String.format(TEMPLATED_LOG4J_FULLPATH, this.version),
        this.tmpDir.getAbsolutePath() + LOG4J_FILENAME,
        context);

    ProcessBuilder pb = new ProcessBuilder(
        String.format(KAFKA_RUN_CLASS_SH, this.version), //"servers/0.8.1/kafka-bin/bin/kafka-run-class.sh",
        JAVA_MAIN, // "org.apache.zookeeper.server.quorum.QuorumPeerMain",
        tmpDir.getAbsolutePath() + PROPERTIES_FILENAME
        );
    Map<String, String> env = pb.environment();
    System.out.println(this.tmpDir.getAbsolutePath() + LOG4J_FILENAME);
    env.put("KAFKA_LOG4J_OPTS", "-Dlog4j.configuration=file:" + this.tmpDir.getAbsolutePath()
        + LOG4J_FILENAME);

    this.proc = pb.start();

    BufferedReader br = new BufferedReader(new InputStreamReader(this.proc.getInputStream()));
    String line = null;
    //block until we see the regex string
    while ((line = br.readLine()) != null) {
     System.out.println(line);
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

  @Override
  public void install() {
    // TODO Auto-generated method stub

  }
}
