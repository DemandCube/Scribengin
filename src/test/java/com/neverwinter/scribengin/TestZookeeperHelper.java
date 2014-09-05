package com.neverwinter.scribengin;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;




import com.google.common.base.Stopwatch;
import com.neverwinter.scribengin.zookeeper.ZookeeperHelper;

//Note that test takes ~1 minute since we restart cluster after every test
public class TestZookeeperHelper {

	private static final Logger logger = Logger
			.getLogger(TestZookeeperHelper.class);
	private final int kafkaListenPort = 9091;
	private final String kafkaLogDir = "/tmp/test";
	private final int zkListenPort = 2182;
	private final int kafkaInstance = 3;
	private final String topic = "scribengin";
	private SampleCluster cluster;
	private ZookeeperHelper zkHelper;
	private String zkURL;
	private static Stopwatch stopwatch;

	@BeforeClass
	public static void init() {
		stopwatch = Stopwatch.createStarted();
	}

	@Before
	public void startCluster() {
		cluster = new SampleCluster(zkListenPort, kafkaInstance,
				kafkaListenPort, kafkaLogDir);
		cluster.start();
		zkURL = cluster.getzkURL();
		cluster.injectTestData(topic);
		try {
			zkHelper = new ZookeeperHelper(zkURL);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		try { // let cluster warm up
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testGetLeaderForTopic() {
		// leave one server up
		for (int i = 0; i < cluster.getKafkaServers().size() - 1; i++) {
			cluster.getKafkaServers().get(i).shutdown();
		}

		String leader = "";
		try {
			Thread.sleep(2000);
			leader = zkHelper.getLeaderForTopicAndPartition(topic, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		assertEquals("127.0.0.1:9093", leader);

		// stop all kafka servers
		for (int i = 0; i < cluster.getKafkaServers().size(); i++) {
			cluster.getKafkaServers().get(i).shutdown();
		}

		try {
			leader = zkHelper.getLeaderForTopicAndPartition(topic, 0);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals("", leader);
	}

	@Test
	public void testGetBrokersForTopicAndPartition() {
		List<String> brokers = null;

		try {
			brokers = zkHelper.getBrokersForTopicAndPartition(topic, 0);
			 assertEquals(1, brokers.size());
		} catch (Exception e) {
		  assert(false);
			e.printStackTrace();
		}
	

		try {
			brokers = zkHelper.getBrokersForTopicAndPartition("giberish", 0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertEquals(0, brokers.size());
	}

	@After
	public void stopCluster() {
		cluster.stop();
		logger.debug("It took us " + stopwatch);
		stopwatch.reset();
	}
}