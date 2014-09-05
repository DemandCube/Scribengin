package com.neverwinter.scribengin;

import java.net.InetAddress;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

//a clustered application that connects to a zookeper and.

// the cluster members 
public class Server3 implements LeaderSelectorListener {

	private static final Logger logger = Logger.getLogger(Server3.class);
	final static RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
	static String zkConnectString = "192.168.33.33:2181";
	static CuratorFramework client;
	LeaderSelector leaderSelector;
	String zkPath = "/scribengin/";

	public static void main(String[] args) {
		BasicConfigurator.configure();
		Server3 server = new Server3();
		
		try {
			server.connect();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void connect() throws Exception {
		client = CuratorFrameworkFactory
				.newClient(zkConnectString, retryPolicy);

		this.leaderSelector = new LeaderSelector(client, zkPath, this);
		client.start();
		client.blockUntilConnected();
		logger.debug(client.getState());
		// TODO Auto-generated method stub
		logger.info("becomeLeader :: connect to ZK [" + zkConnectString + "]");
		leaderSelector.setId(InetAddress.getLocalHost().getHostName());
		logger.info(InetAddress.getLocalHost().getHostName());
		leaderSelector.start();
		logger.info("started the LeaderSelector");
		Thread.sleep(10000);
		logger.debug("haha "+ client.getState());
	}

	@Override
	public void stateChanged(CuratorFramework client, ConnectionState newState) {
		logger.debug("new state "+ newState);

	}

	@Override
	public void takeLeadership(CuratorFramework client) throws Exception {
		

	}
}
