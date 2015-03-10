package com.neverwinterdp.kafka.producer;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.server.kafka.KafkaCluster;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.util.text.TabularFormater;

@RunWith(Parameterized.class)
public class AckKafkaWriterPerfomanceTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  //These are the params you want to manually change
  private static int zkBrokers = 1;
  private static int kafkaBrokers = 3;
  private static int writers = 5;
  private static int runDuration = 10; //SECONDS
  //for the sake of peace avoid a delay of < 10 ms
  private int delay = 10;//MILLISECONDS

  private static int counter = -1;
  private static List<Integer[]> data = new LinkedList<>();
  private static String zkURL;
  private static KafkaCluster cluster;
  KafkaTool kafkaTool;
  private DefaultCallback callback = new DefaultCallback();
  private KafkaMessageSendTool sender;

  private String topic;
  private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(writers);

  //the following variables are changed on each run
  private int dataSize; //Kbytes
  private int numPartitions;
  private FixedMessageSizeGenerator messageGenerator;

  //TODO investigate timeout
  @Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { 1, 10 },
        { 10, 10 },
        { 2, 100 },
        { 20, 100 },
        { 4, 150 },
        { 4, 200 },
        { 4, 500 },
    });
  }

  //Constructor is called for every iteration
  public AckKafkaWriterPerfomanceTest(int numPartitions, int dataSize) {
    this.numPartitions = numPartitions;
    this.dataSize = dataSize;
    messageGenerator = new FixedMessageSizeGenerator(dataSize);
    data.add(new Integer[9]);
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    FileUtil.removeIfExist("./build/kafka", false);
    cluster = new KafkaCluster("./build/kafka", zkBrokers, kafkaBrokers);
    cluster.setReplication(2);
    cluster.start();
    zkURL = cluster.getZKConnect();
  }

  @Before
  public void setUp() throws Exception {

    topic = TestUtils.createRandomTopic();
    kafkaTool = new KafkaTool(topic, zkURL);
    kafkaTool.connect();
    kafkaTool.createTopic(topic, kafkaBrokers, numPartitions);
    callback.resetCounters();
  }

  @After
  public void tearDown() throws Exception {
    scheduler.shutdownNow();
    kafkaTool.close();
  }

  @Test
  public void testWriteToFailedLeader() {
    //  System.out.println("dataSize " + dataSize);
    counter++;
    List<String> messages = new ArrayList<>();

    // {writers} writers, writing every {delay} seconds for {runDuration} seconds

    String brokerURL = "";
    try {
      brokerURL = kafkaTool.getKafkaBrokerList();
    } catch (KeeperException | InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    for (int i = 0; i < writers; i++) {
      sender = new KafkaMessageSendTool(topic, brokerURL, messageGenerator, callback);

      final ScheduledFuture<?> timeHandle = scheduler.scheduleAtFixedRate(sender, delay, delay, TimeUnit.MILLISECONDS);

      scheduler.schedule(new Runnable() {
        public void run() {
          timeHandle.cancel(false);
          //       scheduler.shutdown();
        }
      }, runDuration, TimeUnit.SECONDS);
    }
    //sleep for writer threads to finish running
    try {
      Thread.sleep((runDuration * 1000) + 1000);
      messages = TestUtils.readMessages(topic, zkURL);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    int expected = (1000 / delay * runDuration * writers);
    int written = messageGenerator.getCount();
    int success = callback.getSuccessCount();
    int failed = callback.getFailedCount();
    int read = messages.size();
    int messagesPerSecond = written / runDuration;

    data.get(counter)[0] = expected;
    data.get(counter)[1] = written;
    data.get(counter)[2] = success;
    data.get(counter)[3] = failed;
    data.get(counter)[4] = read;
    data.get(counter)[5] = dataSize;
    data.get(counter)[6] = messagesPerSecond;
    data.get(counter)[7] = numPartitions;
    
    assertEquals(expected, read);
  }

  @AfterClass
  //TODO confirm why writer is slower for  bigger messages?
  //TODO print out Producer configs
  // block.on.buffer.full.
  //buffer.memory
  public static void printreport() throws IOException {
    System.out.println("Test Parameters");
    System.out.println("num.writer = " + writers);
    System.out.println("run duration = " + runDuration);

    System.out.println();

    System.out.println("Broker Configs");
    System.out.println("===============");
    //TODO get broker configs
    /*  KafkaConfig brokerConfigs = servers.getKafkaServers().get(0).config();
      System.out.println("auto.leader.rebalance.enable = " + brokerConfigs.autoLeaderRebalanceEnable());
      System.out.println("controlled shutdown enabled = " + brokerConfigs.controlledShutdownEnable());
      System.out.println("unclean.leader.election.enabled = " + brokerConfigs.uncleanLeaderElectionEnable());
      */System.out.println();
    String[] header = { "run", "expected", "written", "success", "failed", "read", "Partitions ", "datasize (KB)",
        "messages/Second" };
    TabularFormater formater = new TabularFormater(header);
    for (int i = 0; i < data.size(); i++) {
      formater
          .addRow(i, data.get(i)[0], data.get(i)[1], data.get(i)[2], data.get(i)[3], data.get(i)[4], data.get(i)[7],
              data.get(i)[5], data.get(i)[6]);
    }
    System.out.println(formater.getFormatText());
    cluster.shutdown();
  }
}
