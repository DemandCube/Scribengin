package com.neverwinterdp.scribengin;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;

import kafka.cluster.Broker;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.server.KafkaServer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.beust.jcommander.JCommander;


// @RunWith(PowerMockRunner.class)
// @PrepareForTest({ ScribeCommitLog.class })
public class ScribeConsumerTest {
  private static String MINI_CLUSTER_PATH = "/tmp/miniCluster";


  private static final String topic = "scribe";
  private static final int kafkaStartPort = 9091;
  private static final String leader = "127.0.0.1:" + kafkaStartPort;
  private static ScribeConsumer scribeConsumer;
  private static SampleKafkaCluster kafkaCluster;
  private static String[] args = {"--topic", topic, "--leader", leader,
      "--checkpoint_interval", "5000", "--partition", "0"};


  private static String kafkaLogDir = "/tmp/kafka-log";


  private static final Logger logger = Logger
      .getLogger(ScribeConsumerTest.class);

  //public ScribeConsumerTest(String name)
  //{
  //super(name);
  //}

  //public static Test suite()
  //{
  //return new TestSuite( ScribeConsumerTest.class );
  //}

  private FileSystem getMiniCluster() {
    FileSystem fs = null;
    try {
      fs = UnitTestCluster.instance(MINI_CLUSTER_PATH).build();
    } catch (IOException e) {
      assert (false); //wtf?
    }
    return fs;
  }

  private String getPreCommitDirStr(ScribeConsumer consumer) {
    String r = null;
    try {
      Field field = ScribeConsumer.class.getDeclaredField("PRE_COMMIT_PATH_PREFIX");
      field.setAccessible(true);
      r = (String) field.get(consumer);
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
      assert (false);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
      assert (false);
    }
    return r;
  }


  @Ignore
  @Test
  public void testGetLatestOffsetFromCommitLog__corrupted_log_file()
      throws IOException, NoSuchFieldException, IllegalAccessException, NoSuchAlgorithmException,
      NoSuchMethodException, InvocationTargetException, Exception
  {
    ScribeCommitLog log = ScribeCommitLogTestFactory.instance().build();

    log.record(11, 22, "/src/path/data.1", "/dest/path/data.1"); //fs is close

    // create a log with bad checksum
    log = ScribeCommitLogTestFactory.instance().build();
    ScribeCommitLogTestFactory.instance().addCorruptedEntry(
        log, 23, 33,
        "/src/path/data.2", "/dest/path/data.2", true);

    ScribeConsumer sc = new ScribeConsumer();
    sc.setScribeCommitLogFactory(ScribeCommitLogTestFactory.instance());
    sc.setFileSystemFactory(UnitTestCluster.instance(MINI_CLUSTER_PATH));

    Method mthd = ScribeConsumer.class.getDeclaredMethod("getLatestOffsetFromCommitLog");
    mthd.setAccessible(true);
    long offset = (Long) mthd.invoke(sc);
    Assert.assertTrue(offset == 22);
  }

  @Ignore
  @Test
  public void testGetLatestOffsetFromCommitLog__data_has_been_committed()
      throws IOException, NoSuchFieldException, IllegalAccessException, NoSuchAlgorithmException,
      NoSuchMethodException, InvocationTargetException, Exception
  {
    ScribeCommitLog log = ScribeCommitLogTestFactory.instance().build();

    log.record(11, 22, "/src/path/data.1", "/dest/path/data.1"); //fs is close
    ScribeConsumer sc = new ScribeConsumer();
    sc.setScribeCommitLogFactory(ScribeCommitLogTestFactory.instance());
    sc.setFileSystemFactory(UnitTestCluster.instance(MINI_CLUSTER_PATH));

    Method mthd = ScribeConsumer.class.getDeclaredMethod("getLatestOffsetFromCommitLog");
    mthd.setAccessible(true);
    long offset = (Long) mthd.invoke(sc);
    Assert.assertTrue(offset == 22);
  }

  @Ignore
  @Test
  public void testGetLatestOffsetFromCommitLog__tmpDataFile_does_not_match_log()
      throws IOException, NoSuchFieldException, IllegalAccessException, NoSuchAlgorithmException,
      NoSuchMethodException, InvocationTargetException, Exception
  {
    ScribeCommitLog log = ScribeCommitLogTestFactory.instance().build();

    log.record(11, 22, "/tmp/scribe.data.1", "/dest/path/data.1"); //fs is close
    FileSystem fs = UnitTestCluster.instance(MINI_CLUSTER_PATH).build();

    String mismatchedPath = "/tmp/scribe.data.mismatched";
    FSDataOutputStream os = fs.create(new Path(mismatchedPath));
    os.write("dummy data".getBytes());
    os.write('\n');
    try {
      os.close();
    } catch (IOException e) {
      assert (false);
    }

    Assert.assertTrue(fs.exists(new Path(mismatchedPath)));

    ScribeConsumer sc = new ScribeConsumer();
    sc.setScribeCommitLogFactory(ScribeCommitLogTestFactory.instance());
    sc.setFileSystemFactory(UnitTestCluster.instance(MINI_CLUSTER_PATH));

    Method mthd = ScribeConsumer.class.getDeclaredMethod("getLatestOffsetFromCommitLog");
    mthd.setAccessible(true);
    long offset = (Long) mthd.invoke(sc);
    Assert.assertTrue(offset == 22);

    // make sure that the data file is cleaned up
    fs = UnitTestCluster.instance(MINI_CLUSTER_PATH).build();
    Assert.assertFalse(fs.exists(new Path(mismatchedPath)));
  }

  @Ignore
  @Test
  public void testGetLatestOffsetFromCommitLog__commit_uncommitted_tmp_data()
      throws IOException, NoSuchFieldException, IllegalAccessException, NoSuchAlgorithmException,
      NoSuchMethodException, InvocationTargetException, Exception
  {
    ScribeCommitLog log = ScribeCommitLogTestFactory.instance().build();

    String uncommittedDataPath = "/tmp/scribe.data.1";
    String committedDataPath = "/tmp/scribe.data.1.committed";
    log.record(11, 22, uncommittedDataPath, committedDataPath); //fs is close
    FileSystem fs = UnitTestCluster.instance(MINI_CLUSTER_PATH).build();

    FSDataOutputStream os = fs.create(new Path(uncommittedDataPath));
    os.write("dummy data".getBytes());
    os.write('\n');
    try {
      os.close();
    } catch (IOException e) {
      assert (false);
    }

    Assert.assertTrue(fs.exists(new Path(uncommittedDataPath)));

    ScribeConsumer sc = new ScribeConsumer();
    sc.setScribeCommitLogFactory(ScribeCommitLogTestFactory.instance());
    sc.setFileSystemFactory(UnitTestCluster.instance(MINI_CLUSTER_PATH));

    Method mthd = ScribeConsumer.class.getDeclaredMethod("getLatestOffsetFromCommitLog");
    mthd.setAccessible(true);
    long offset = (Long) mthd.invoke(sc);
    Assert.assertTrue(offset == 22);

    //make sure that the data file is moved
    fs = UnitTestCluster.instance(MINI_CLUSTER_PATH).build();
    Assert.assertFalse(fs.exists(new Path(uncommittedDataPath)));
    Assert.assertTrue(fs.exists(new Path(committedDataPath)));
  }

  @Test
  public void testGetNewLeader() throws IOException, NoSuchMethodException, SecurityException,
      IllegalAccessException, IllegalArgumentException, InvocationTargetException {

    Method method = ScribeConsumer.class.getDeclaredMethod("createNewConsumer");
    method.setAccessible(true);

    SimpleConsumer consumer1 = (SimpleConsumer) method.invoke(scribeConsumer);
    Broker broker = null;
    for (int i = 0; i < kafkaCluster.getKafkaServers().size(); i++) {
      if (kafkaCluster.getKafkaServers().get(i).config().port() == consumer1.port()) {
        broker = new Broker(i, consumer1.host(), consumer1.port());
      }
    }
    logger.info("consumer1 " + consumer1);
    //assert that leader is one of brokers
    Assert.assertTrue("Leader is not one of brokers.",
        kafkaCluster.getKafkaBrokers().contains(broker));
    //change leadership
    switchKafkaLeader(consumer1.host(), consumer1.port());
    SimpleConsumer consumer2 = (SimpleConsumer) method.invoke(scribeConsumer);
    logger.info("consumer1 port " + consumer1.port() + " consumer2 port " + consumer2.port());
    assertThat(consumer1.port(), not(equalTo(consumer2.port())));

  }

  private void switchKafkaLeader(String hostname, int port) {
    for (KafkaServer server : kafkaCluster.getKafkaServers()) {
      if (server.config().port() == port && server.config().hostName().equals(hostname)) {
        logger.info("Shutting down " + server.config().port() + " " + port);
        server.shutdown();
        server.awaitShutdown();
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @BeforeClass
  public static void setup() {
    scribeConsumer = new ScribeConsumer();
    JCommander jc = new JCommander(scribeConsumer);
    jc.addConverterFactory(new CustomConvertFactory());
    jc.parse(args);
    try {
      startKafkaCluster();
      scribeConsumer.setScribeCommitLogFactory(ScribeCommitLogTestFactory.instance());
      scribeConsumer.setFileSystemFactory(UnitTestCluster.instance(MINI_CLUSTER_PATH));
      scribeConsumer.init();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void startKafkaCluster() {
    kafkaCluster = new SampleKafkaCluster(2182, 3, kafkaStartPort, kafkaLogDir);
    kafkaCluster.start();
    kafkaCluster.injectTestData(topic);
  }

  @AfterClass
  public static void tearDown() {
    try {
      FileUtils.deleteDirectory(new File(kafkaLogDir));
    } catch (IOException e) {
      //Quietly
    }
  }

}
