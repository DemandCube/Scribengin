package com.neverwinterdp.scribengin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

public class KafkaFixture extends Fixture {
  private static int nextBrokerId =1;
  private Process proc;
  private String host;
  private int port;
  private String zkHost;
  private int zkPort;
  private String version;
  private int brokerId;
  private static final String PROPERTIES_FILENAME = "/kafka.properties";
  private static final String LOG4J_FILENAME = "/log4j.properties";
  private static final String TEMPLATED_LOG4J_FULLPATH = "servers/%s/resources" + LOG4J_FILENAME;

  private static final String TEMPLATED_PROPERTIES_FULLPATH = "servers/%s/resources" + PROPERTIES_FILENAME;
  private static final String JAVA_MAIN = "kafka.Kafka";
  private static final String WAIT_FOR_REGEX = ".*?started.*?";

  private static int incAndGetId() {
    int r = nextBrokerId;
    nextBrokerId++;
    return r;
  }

  public KafkaFixture( String version,
      String kafkaHost, int kafkaPort,
      String zkHost, int zkPort ) throws IOException {
    super();
    this.host = kafkaHost;
    this.port = kafkaPort;
    this.zkHost = zkHost;
    this.zkPort = zkPort;
    this.version = version;
    this.brokerId = incAndGetId();
  }

  public void start() throws IOException {
    System.out.println(this.tmpDir.getAbsolutePath() + PROPERTIES_FILENAME); //xxx

    HashMap<String, String> context = new HashMap<String, String>();
    context.put("broker_id", Integer.toString(brokerId));
    context.put("host", host);
    context.put("port", Integer.toString(port));
    context.put("zk_host", zkHost);
    context.put("zk_port", Integer.toString(zkPort));
    context.put("partitions", Integer.toString(1));
    context.put("replicas", Integer.toString(1));
    context.put("tmp_dir", tmpDir.getAbsolutePath());

    this.renderConfig(
      String.format(TEMPLATED_PROPERTIES_FULLPATH, this.version),
      this.tmpDir.getAbsolutePath() + PROPERTIES_FILENAME,
      context
    );

    this.renderConfig(
      String.format(TEMPLATED_LOG4J_FULLPATH, this.version),
      this.tmpDir.getAbsolutePath() + LOG4J_FILENAME,
      context );

    ProcessBuilder pb = new ProcessBuilder(
      String.format(KAFKA_RUN_CLASS_SH, this.version),  //"servers/0.8.1/kafka-bin/bin/kafka-run-class.sh",
      JAVA_MAIN,                                        // "org.apache.zookeeper.server.quorum.QuorumPeerMain",
      tmpDir.getAbsolutePath() + PROPERTIES_FILENAME
    );
    Map<String, String> env = pb.environment();
    System.out.println(this.tmpDir.getAbsolutePath() + LOG4J_FILENAME);
    env.put("KAFKA_LOG4J_OPTS", "-Dlog4j.configuration=file:" + this.tmpDir.getAbsolutePath() + LOG4J_FILENAME);

    this.proc =  pb.start();

    BufferedReader br = new BufferedReader(new InputStreamReader(this.proc.getInputStream()));
    String line = null;
    while ((line = br.readLine()) != null) {
      if (line.matches(WAIT_FOR_REGEX)) {
        break;
      }
    }

  }

  public void stop() throws IOException {
    this.proc.destroy();
    // clean up the tmp directory
    FileUtils.deleteDirectory(this.tmpDir);
    nextBrokerId--;
  }

}

