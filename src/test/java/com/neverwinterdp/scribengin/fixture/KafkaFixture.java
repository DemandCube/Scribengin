package com.neverwinterdp.scribengin.fixture;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class KafkaFixture extends Fixture {
  private static int nextBrokerId = 1;
  private Process proc;
  private String zkHost;
  private int zkPort;
  private int brokerId;
  private static final String PROPERTIES_FILENAME = "/kafka.properties";
  private static final String LOG4J_FILENAME = "/log4j.properties";
  private static final String TEMPLATED_LOG4J_FULLPATH = "servers/%s/resources" + LOG4J_FILENAME;

  private static final String TEMPLATED_PROPERTIES_FULLPATH = "servers/%s/resources"
      + PROPERTIES_FILENAME;
  private static final String JAVA_MAIN = "kafka.Kafka";
  private static final String WAIT_FOR_REGEX = ".*?started.*?";

  private static final Logger logger = Logger.getLogger(KafkaFixture.class);

  private static int incAndGetId() {
    int r = nextBrokerId;
    nextBrokerId++;
    return r;
  }

  public KafkaFixture(String version,
      String kafkaHost, int kafkaPort,
      String zkHost, int zkPort) throws IOException {
    super();
    this.host = kafkaHost;
    this.port = kafkaPort;
    this.zkHost = zkHost;
    this.zkPort = zkPort;
    this.version = version;
    this.brokerId = incAndGetId();
  }

  @Override
  public void start() throws IOException {
    System.out.println("Starting kafka ");
    HashMap<String, String> context = new HashMap<String, String>();
    context.put("broker_id", Integer.toString(brokerId));
    context.put("host", host);
    context.put("port", Integer.toString(port));
    context.put("zk_host", zkHost);
    context.put("zk_port", Integer.toString(zkPort));
    context.put("partitions", Integer.toString(1));
    context.put("replicas", Integer.toString(1));
    context.put("tmp_dir", tmpDir.getAbsolutePath());
    System.out.println("Before crucial try");
    try {
      this.renderConfig(
          String.format(TEMPLATED_PROPERTIES_FULLPATH, this.version),
          this.tmpDir.getAbsolutePath() + PROPERTIES_FILENAME,
          context
          );

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
      while ((line = br.readLine()) != null) {
        System.out.println(line);
        if (line.matches(WAIT_FOR_REGEX)) {
          break;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void stop() throws IOException {
    this.proc.destroy();
    // clean up the tmp directory
    FileUtils.deleteDirectory(this.tmpDir);
    nextBrokerId--;
  }

  @Override
  public void install() throws IOException {
    logger.info("Installing kafka ");
    Process p = Runtime.getRuntime().exec("script/bootstrap_kafka.sh servers");
    try {
      p.waitFor();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(p.getInputStream()));

    String line = "";
    while ((line = reader.readLine()) != null) {
      System.out.println(line);//xxx
    }
    System.out.println("Finished installing kafka.");
  }
}
