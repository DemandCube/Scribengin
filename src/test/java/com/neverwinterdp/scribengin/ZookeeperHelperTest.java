package com.neverwinterdp.scribengin;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.utils.SampleKafkaCluster;

// Recreates cluster for every test. takes longer but tests can safely run in any order.
public class ZookeeperHelperTest {

	private final int kafkaListenPort = 9090;
	private final String kafkaLogDir = "/tmp/kafka-logs";
	private final int zkListenPort = 2180;
	private final int kafkaInstance = 3;
	private final String topic = "scribengin";
	private SampleKafkaCluster cluster;
	private ZookeeperHelper zkHelper;
	private String zkURL;

	@Before
	public void startCluster() {

		cluster = new SampleKafkaCluster(zkListenPort, kafkaInstance,
				kafkaListenPort, kafkaLogDir);
		// cluster.setUseExternalZk(true);
		// cluster.setZookeeperConnectURL("192.168.33.33:2181");
		cluster.start();
		zkURL = cluster.getZookeeperConnectURL();
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
			Thread.sleep(7000);
			leader = zkHelper.getLeaderForTopicAndPartition(topic, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// the last remaining server should be the leader
		assertEquals("127.0.0.1:9092", leader);

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
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertEquals(1, brokers.size());

		try {
			brokers = zkHelper.getBrokersForTopicAndPartition("giberish", 0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertEquals(0, brokers.size());
	}

	@Test
	public void testGetLeaderHostPort() {
		Set<HostPort> allBrokers = new HashSet<HostPort>();
		allBrokers.add(new HostPort("127.0.0.1", 9090));
		allBrokers.add(new HostPort("127.0.0.1", 9091));
		allBrokers.add(new HostPort("127.0.0.1", 9092));

		HostPort actual = null;
		try {
			actual = zkHelper.getLeaderHostPort(topic, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		assertTrue(allBrokers.contains(actual));
	}

	@After
	public void stopCluster() {
		cluster.stop();
	}
}