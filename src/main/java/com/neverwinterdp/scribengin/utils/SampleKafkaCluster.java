package com.neverwinterdp.scribengin.utils;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;

import org.apache.curator.test.TestingServer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

//TODO FutureTask instead of sleep
public class SampleKafkaCluster {

	private int kafkaListenPort;
	private String kafkaLogDir;
	private int zkListenPort;
	private int kafkaInstances;

	private static final Logger logger = Logger
			.getLogger(SampleKafkaCluster.class);
	private String zookeeperConnectURL;

	public String getZookeeperConnectURL() {
		return zookeeperConnectURL;
	}

	public void setZookeeperConnectURL(String zookeeperConnectURL) {
		this.zookeeperConnectURL = zookeeperConnectURL;
	}

	private boolean useExternalZk;

	public boolean isUseExternalZk() {
		return useExternalZk;
	}

	public void setUseExternalZk(boolean useExternalZk) {
		this.useExternalZk = useExternalZk;
	}

	private static TestingServer testingServer;
	private static List<KafkaServer> kafkaServers;

	public List<KafkaServer> getKafkaServers() {
		return kafkaServers;
	}

	public static void setKafkaServers(List<KafkaServer> kafkaServers) {
		SampleKafkaCluster.kafkaServers = kafkaServers;
	}

	public SampleKafkaCluster(int zkListenPort, int kafkaInstances,
			int kafkaListenPort, String kafkaLogDir) {
		this.zkListenPort = zkListenPort;
		this.kafkaListenPort = kafkaListenPort;
		this.kafkaLogDir = kafkaLogDir;
		this.kafkaInstances = kafkaInstances;
		kafkaServers = new LinkedList<KafkaServer>();
	}

	// returns its connect URL
	public void startZookeeper() throws Exception {
		if (useExternalZk) {
			return;
		}
		testingServer = new TestingServer(zkListenPort, true);
		logger.debug("Testing server temp dir "
				+ testingServer.getTempDirectory());
		testingServer.start();
		Thread.sleep(2000);
		zookeeperConnectURL = testingServer.getConnectString();
	}

	public List<KafkaServer> startKafkaInstances() {
		Preconditions.checkState(zookeeperConnectURL != null,
				"Zookeeper server has not yet been started.");

		File file = new File(kafkaLogDir);
		boolean success = file.delete();
		logger.debug("Tumetoboa " + success);
		Properties props;
		KafkaServer server;
		for (int i = 0; i < kafkaInstances; i++) {
			props = new Properties();
			props.setProperty("hostname", "127.0.0.1");
			props.setProperty("host.name", "127.0.0.1");
			props.setProperty("port", Integer.toString(kafkaListenPort + i));
			props.setProperty("broker.id", Integer.toString(i));
			props.setProperty("auto.create.topics.enable", "true");
			props.setProperty("log.dirs", kafkaLogDir + "/" + i);
			props.setProperty("enable.zookeeper", "true");
			props.setProperty("zookeeper.connect", zookeeperConnectURL);
			// props.setProperty("default.replication.factor", "1");
			server = new KafkaServer(new KafkaConfig(props), new MockTime());
			server.startup();
			kafkaServers.add(server);
		}
		return kafkaServers;
	}

	public void stopZookeeper() throws IOException {
		if (testingServer == null)
			return;
		testingServer.stop();
		testingServer = null;
	}

	public void stopKafka() {
		for (KafkaServer kafkaServer : kafkaServers) {
			kafkaServer.shutdown();
			kafkaServer.awaitShutdown();
			kafkaServer = null;
		}
	}

	static public class MockTime implements Time {

		public long milliseconds() {
			return System.currentTimeMillis();
		}

		public long nanoseconds() {
			return System.nanoTime();
		}

		public void sleep(long time) {
			try {
				Thread.sleep(time);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void start() {
		try {
			startZookeeper();
			startKafkaInstances();
			Thread.sleep(5000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void stop() {
		try {
			stopKafka();
			stopZookeeper();
			Thread.sleep(10000);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void injectTestData(String topic) {

		Properties props = new Properties();
		props.put("metadata.broker.list", getKafkaURLs());
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);

		for (int events = 0; events < 200; events++) {
			String msg = UUID.randomUUID().toString();
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					topic, msg);
			producer.send(data);
		}
		producer.close();
	}

	private String getKafkaURLs() {
		StringBuilder builder = new StringBuilder();
		for (KafkaServer server : kafkaServers) {
			builder.append(server.config().advertisedHostName());
			builder.append(":");
			builder.append(server.config().advertisedPort());
			builder.append(",");
		}
		logger.debug("shida " + builder.substring(0, builder.length() - 1));
		return builder.substring(0, builder.length() - 1);
	}
}
