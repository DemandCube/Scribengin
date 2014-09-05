package com.neverwinter.scribengin.zookeeper;

import java.util.Collection;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.google.common.util.concurrent.Service;

// get status of each cluster member
// get count of cluster members
// read zookeeper, get cluster members for zkpath
// get status of members of cluster
// TODO read https://github.com/rhavyn/norbert
public class ZookeeperClusterManager {

  private String zkPath;

  Collection<Service> services;
  private String zkConnectString;
  final static RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
  private CuratorFramework zkClient;

  public ZookeeperClusterManager(String zkPath) {
    super();
    this.zkPath = zkPath;
  }

  private void init() {
    zkClient = CuratorFrameworkFactory.newClient(
        zkConnectString, retryPolicy);
  }

  public Collection<Service> getClusterMembers(String zkPath) throws Exception {
    //	Iterable<String>  members = zkClient.getChildren().forPath(zkPath);
    //todo convert members to services

    return services;
  }


  void addClusterMember(Service service) {
    services.add(service);
  }

  void shutdownCluster(Collection<Service> clusterMembers) {
    for (Service service : clusterMembers) {
      if (service.isRunning()) {
        service.stopAsync();
        service.awaitTerminated();
      }
    }
  }

}
