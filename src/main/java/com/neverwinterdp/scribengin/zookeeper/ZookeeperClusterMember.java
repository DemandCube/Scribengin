package com.neverwinterdp.scribengin.zookeeper;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Service.State;
import com.neverwinterdp.scribengin.api.ClusterMember;

// TODO Test register self nicely
// Zookeeper cluster member that can also be leader of cluster (Scribenging has no concept of
// leader?)
// TODO test leader selection
// TODO mechanism for temporarily de-registering.

public class ZookeeperClusterMember implements ClusterMember,
    LeaderSelectorListener {

  private CuratorFramework zkClient;
  private String zkPath;
  private String name;
  private static EnumConverter converter = new EnumConverter();
  private final LeaderSelector leaderSelector;
  private RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

  private static final Logger logger = Logger
      .getLogger(ZookeeperClusterMember.class);

  public ZookeeperClusterMember(String zkConnectString, String path,
      String name) {

    this.zkClient =
        CuratorFrameworkFactory.builder()
            .connectString(zkConnectString)
            .retryPolicy(retryPolicy)
            .build();

    try {
      if (name != null) {
        this.name = name;
      } else {
        this.name = InetAddress.getLocalHost().getHostAddress();
      }
      logger.debug("name " + this.name);
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    this.zkPath = path + "/" + this.name;
    logger.debug("zkPath " + zkPath);
    logger.debug("path " + path);

    leaderSelector = new LeaderSelector(zkClient, path, this);
    leaderSelector.setId(this.name);
  }

  @Override
  public ListenableFuture<State> start() {
    logger.debug("STARTING  =====================>");
    zkClient.start();
    leaderSelector.start();
    leaderSelector.autoRequeue();

    //TODO be sure of what is happening here.
    try {
      zkClient.newNamespaceAwareEnsurePath(zkPath).ensure(zkClient.getZookeeperClient());
      logger.debug(" registered <><><><><><><>"
          + zkClient.create().withProtection()
              .withMode(CreateMode.EPHEMERAL).forPath(zkPath));
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public State startAndWait() {
    start();
    try {
      zkClient.blockUntilConnected();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    logger.debug("startedn -------------------->");
    return converter.apply(zkClient.getState());
  }

  @Override
  public Service startAsync() {
    // all methods in start are async
    start();
    return this;
  }

  @Override
  public boolean isRunning() {
    return zkClient.getState() == CuratorFrameworkState.STARTED;
  }

  @Override
  public State state() {
    return converter.apply(zkClient.getState());
  }

  @Override
  public ListenableFuture<State> stop() {
    try {

      // TODO Only delete if this client is leader
      // this.leaderSelector.hasLeadership()
      // i.e is the last node in cluster
      zkClient.delete().guaranteed().deletingChildrenIfNeeded()
          .inBackground().forPath(zkPath);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    leaderSelector.close();
    zkClient.close();
    return null;
  }

  @Override
  public State stopAndWait() {
    try {
      zkClient.delete().guaranteed().deletingChildrenIfNeeded()
          .forPath(zkPath);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    leaderSelector.close();
    zkClient.close();
    return converter.apply(zkClient.getState());
  }

  @Override
  public Service stopAsync() {
    stop();
    return this;
  }

  @Override
  public void awaitRunning() {
    try {
      zkClient.blockUntilConnected();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void awaitRunning(long timeout, TimeUnit unit)
      throws TimeoutException {
    try {
      zkClient.blockUntilConnected((int) timeout, unit);
    } catch (InterruptedException e) {
      logger.error(e.getLocalizedMessage(), e);
      throw new TimeoutException();
    }
  }

  @Override
  public void awaitTerminated() {

    while (zkClient.getState() != CuratorFrameworkState.STOPPED) {
      try {
        wait(0);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  @Override
  public void awaitTerminated(long timeout, TimeUnit unit)
      throws TimeoutException {
    zkClient.delete().deletingChildrenIfNeeded();
  }

  @Override
  public Throwable failureCause() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void addListener(Listener listener, Executor executor) {
    // TODO Auto-generated method stub
  }

  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState) {
    // you MUST handle connection state changes. This WILL happen in
    // production code.
    if ((newState == ConnectionState.LOST)
        || (newState == ConnectionState.SUSPENDED)) {

    }
  }

  @Override
  public void takeLeadership(CuratorFramework client) throws Exception {
    // we are now the leader. This method should not return until we want to
    // relinquish leadership
    logger.info("Became Leader.. starting to do work. " + client.toString());
    while (isRunning()) {
      // logger.debug("isrunning");
    }
  }

  public LeaderSelector getLeaderSelector() {
    return leaderSelector;
  }

  @Override
  public void run() {
    Thread.currentThread().setName("Cluster-Registration-Thread");
    startAndWait();
    //TODO get a mechanism of temporarily stopping
    while (isRunning()) {
    }
  }
}


class EnumConverter implements Function<CuratorFrameworkState, Service.State> {

  @Override
  public State apply(CuratorFrameworkState input) {
    switch (input) {
      case STARTED:
        return Service.State.RUNNING;
      case STOPPED:
        return Service.State.TERMINATED;
      default:
        return Service.State.FAILED;
    }
  }
}
