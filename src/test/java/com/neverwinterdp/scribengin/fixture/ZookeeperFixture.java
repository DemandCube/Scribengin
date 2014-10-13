package com.neverwinterdp.scribengin.fixture;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class ZookeeperFixture extends Fixture {
  private Process proc;
  private static final String PROPERTIES_FILENAME = "/zookeeper.properties";
  private static final String LOG4J_FILENAME = "/log4j.properties";
  private static final String TEMPLATED_LOG4J_FULLPATH = "servers/%s/resources" + LOG4J_FILENAME;

  private static final Logger logger = Logger.getLogger(ZookeeperFixture.class);
  private static final String TEMPLATED_PROPERTIES_FULLPATH = "servers/%s/resources"
      + PROPERTIES_FILENAME;
  private static final String JAVA_MAIN = "org.apache.zookeeper.server.quorum.QuorumPeerMain";
  private static final String WAIT_FOR_REGEX = ".*?Established session.*?";
  private static final String SNAPSHOTTING = ".*?FileTxnSnapLog.*?";

  public ZookeeperFixture(String version, String host, int port) throws IOException {
    super();
    this.host = host;
    this.port = port;
    this.version = version;
  }

  @Override
  public void start() throws IOException {
    logger.info("start. ");
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
    logger.debug(this.tmpDir.getAbsolutePath() + LOG4J_FILENAME);
    env.put("KAFKA_LOG4J_OPTS", "-Dlog4j.configuration=file:" + this.tmpDir.getAbsolutePath()
        + LOG4J_FILENAME);

    this.proc = pb.start();

    BufferedReader br = new BufferedReader(new InputStreamReader(this.proc.getInputStream()));
    String line = null;
    //block until we see the regex string
    while ((line = br.readLine()) != null) {
      System.out.println(line);
      if (line.matches(WAIT_FOR_REGEX) || line.matches(SNAPSHOTTING)) {
        break;
      }
    }
  }

  public void stop() throws IOException {
    logger.info("stop.");
    // Destroy the running process
    this.proc.destroy();
    // clean up the tmp directory
    FileUtils.deleteDirectory(this.tmpDir);
  }

  @Override
  public void install() {
    // Installed when installing kafka
    //TODO do a check then install if needed

  }
}
