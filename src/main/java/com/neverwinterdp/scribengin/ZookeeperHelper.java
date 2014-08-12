package com.neverwinterdp.scribengin;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException.NoNodeException;

import com.neverwinterdp.scribengin.utils.Utils;

public class ZookeeperHelper {

	private String zkConnectString;
	private CuratorFramework zkClient;
	final static RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

	private static final Logger logger = Logger
			.getLogger(ZookeeperHelper.class);
	private String brokerInfoLocation = "/brokers/ids/";
	private String topicInfoLocation = "/brokers/topics/";

	public ZookeeperHelper(String zookeeperURL) throws InterruptedException {
		super();
		zkConnectString = zookeeperURL;
		zkClient = CuratorFrameworkFactory.newClient(zkConnectString,
				retryPolicy);
		init();
	}

	private void init() throws InterruptedException {
		zkClient.start();
		zkClient.blockUntilConnected();
	}

	public HostPort getLeaderHostPort(String topic, int partion) throws Exception {

		String[] values = getLeaderForTopicAndPartition(topic, partion).split(
				":");
		if (values.length == 2)
			return new HostPort(values[0], values[1]);
		else
			return null;
	}

	public String getLeaderForTopicAndPartition(String topic, int partion)
			throws Exception {
		String leader = "";

		PartitionState partitionState = getPartionState(topic, partion);
		int leaderId = partitionState.getLeader();
		byte[] bytes = {};

		try {
			if (leaderId == -1) {
				return leader;
			}
			logger.debug("Going to look for " + brokerInfoLocation + leaderId);
			bytes = zkClient.getData().forPath(brokerInfoLocation + leaderId);
		}

		catch (NoNodeException nne) {
			logger.error(nne.getMessage(), nne);
			return leader;
		}
		Partition part = Utils.toClass(bytes, Partition.class);
		logger.debug("leader " + part);

		return leader.concat(part.getHost()).concat(":")
				.concat(String.valueOf(part.getPort()));

	}

	// brokers/topics/scribe/partitions/0/state
	public List<String> getBrokersForTopicAndPartition(String topic, int partion)
			throws Exception {
		PartitionState partitionState = getPartionState(topic, partion);
		StringBuilder broker = new StringBuilder();
		List<String> brokers = new LinkedList<String>();
		byte[] partitions;
		logger.debug("PartitionState " + partitionState);

		for (Integer b : partitionState.getIsr()) {
			// TODO broker is registered but offline next line throws
			// nonodeexception
			try {
				partitions = zkClient.getData().forPath(brokerInfoLocation + b);
				Partition part = Utils.toClass(partitions, Partition.class);
				broker.append(part.getHost());
				broker.append(":");
				broker.append(part.getPort());
				brokers.add(broker.toString());
				broker.setLength(0);
			} catch (NoNodeException nne) {
				logger.debug(nne.getMessage());
			}
		}
		return brokers;
	}

	// TODO multimap for all topics

	/**
	 * @param topic
	 * @param partion
	 * @return
	 * @throws Exception
	 */
	private PartitionState getPartionState(String topic, int partion)
			throws Exception {

		try {
			zkClient.getData().forPath(topicInfoLocation + topic);
		} catch (NoNodeException nne) {
			// there are no nodes for the topic. We return an empty
			// Partitionstate
			logger.error(nne.getMessage());
			return new PartitionState();
		}
		byte[] bytes = zkClient.getData()
				.forPath(
						topicInfoLocation + topic + "/partitions/" + partion
								+ "/state");
		PartitionState partitionState = Utils.toClass(bytes,
				PartitionState.class);
		return partitionState;
	}

	// where is the info
	// brokers/id and /brokers/topics
	// to be used only when kafka broker info is not stored in default zookeeper
	// location
	public void setBrokerInfoLocation(String brokerInfoLocation) {
		this.brokerInfoLocation = brokerInfoLocation;
	}

	public void setTopicInfoLocation(String topicInfoLocation) {
		this.topicInfoLocation = topicInfoLocation;
	}
}

class Partition {
	String host;
	int jmx_port;
	int port;
	String timestamp;
	int version;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getJmx_port() {
		return jmx_port;
	}

	public void setJmx_port(int jmx_port) {
		this.jmx_port = jmx_port;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	@Override
	public String toString() {
		return "Partition [host=" + host + ", jmx_port=" + jmx_port + ", port="
				+ port + ", timestamp=" + timestamp + ", version=" + version
				+ "]";
	}
}

class PartitionState {
	int controller_epoch;
	Set<Integer> isr;
	int leader;
	int leader_epoch;
	int version;

	public PartitionState() {
		super();
		isr = Collections.emptySet();
	}

	public int getController_epoch() {
		return controller_epoch;
	}

	public void setController_epoch(int controller_epoch) {
		this.controller_epoch = controller_epoch;
	}

	public Set<Integer> getIsr() {
		return isr;
	}

	public void setIsr(Set<Integer> isr) {
		this.isr = isr;
	}

	public int getLeader() {
		return leader;
	}

	public void setLeader(int leader) {
		this.leader = leader;
	}

	public int getLeader_epoch() {
		return leader_epoch;
	}

	public void setLeader_epoch(int leader_epoch) {
		this.leader_epoch = leader_epoch;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	@Override
	public String toString() {
		return "PartitionState [controller_epoch=" + controller_epoch
				+ ", isr=" + isr + ", leader=" + leader + ", leader_epoch="
				+ leader_epoch + ", version=" + version + "]";
	}
}