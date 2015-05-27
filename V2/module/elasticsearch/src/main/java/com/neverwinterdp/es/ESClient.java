package com.neverwinterdp.es;

import java.util.Map;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequestBuilder;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * $Author: Tuan Nguyen$
 **/
public class ESClient {
  protected TransportClient client;
  private String[] address;

  public ESClient(String[] address) {
    this.address = address;
    Settings settings =
        ImmutableSettings.settingsBuilder().put("cluster.name", "neverwinterdp").build();
    client = new TransportClient(settings);
    for (String selAddr : address) {
      int port = 9300;
      if (selAddr.indexOf(":") > 0) {
        port = Integer.parseInt(selAddr.substring(selAddr.indexOf(":") + 1));
        selAddr = selAddr.substring(0, selAddr.indexOf(":"));
      }
      client.addTransportAddress(new InetSocketTransportAddress(selAddr, port));
    }
  }

  public String[] getAddress() { return this.address; }

  public boolean waitForConnected(long timeout) throws InterruptedException {
    long stopTime = System.currentTimeMillis() + timeout ;
    while(System.currentTimeMillis() < stopTime) {
      ImmutableList<DiscoveryNode> nodes  = client.connectedNodes() ;
      if(!nodes.isEmpty()) return true ;
      Thread.sleep(1000);
    }
    return false ;
  }
  
  public void createIndex(String index, String settings) throws Exception {
    CreateIndexRequestBuilder builder = client.admin().indices().prepareCreate(index);
    if (settings != null) {
      builder.setSettings(settings);
    }
    CreateIndexResponse response = builder.execute().actionGet();
  }

  public void optimizeIndex(String index) throws Exception {
    OptimizeRequestBuilder builder = client.admin().indices().prepareOptimize(index);
    OptimizeResponse response = builder.execute().actionGet();
    System.out.println("Optimize Failed Shard: " + response.getFailedShards());
  }

  public void removeIndex(String index) throws Exception {
    DeleteIndexRequestBuilder builder = client.admin().indices().prepareDelete(index);
    DeleteIndexResponse response = builder.execute().actionGet();
  }

  public void updateMapping(String index, String type, String mapping) throws Exception {
    PutMappingRequestBuilder builder = client.admin().indices().preparePutMapping(index);
    builder.setType(type);
    builder.setSource(mapping);
    PutMappingResponse response = builder.execute().actionGet();
  }

  public void updateSettings(String index, String settings) throws Exception {
    UpdateSettingsRequestBuilder builder = client.admin().indices().prepareUpdateSettings(index);
    builder.setSettings(settings);
    UpdateSettingsResponse response = builder.execute().actionGet();
  }

  public ClusterState getClusterState() {
    ClusterStateRequestBuilder stateBuilder = client.admin().cluster().prepareState();
    ClusterStateResponse response = stateBuilder.execute().actionGet();
    ClusterState state = response.getState();
    return state;
  }

  public NodeInfo getNodeInfo(String nodeId) {
    NodesInfoRequestBuilder builder = client.admin().cluster().prepareNodesInfo(nodeId);
    NodesInfoResponse response = builder.execute().actionGet();
    return response.getNodes()[0];
  }

  public NodeInfo[] getNodeInfo(String... nodeId) {
    NodesInfoRequestBuilder builder = client.admin().cluster().prepareNodesInfo(nodeId);
    NodesInfoResponse response = builder.execute().actionGet();
    return response.getNodes();
  }

  public boolean hasIndex(String name) {
    String[] indices = this.getClusterState().metaData().getConcreteAllIndices();
    for (String sel : indices) {
      if (sel.equals(name))
        return true;
    }
    return false;
  }

  public Map<String, IndexStats> getIndexStats() {
    String[] indices = getClusterState().metaData().getConcreteAllIndices();
    IndicesStatsRequestBuilder builder = client.admin().indices().prepareStats(indices);
    IndicesStatsResponse response = builder.execute().actionGet(); 
    Map<String, IndexStats> stats = response.getIndices() ;
    return stats;
  }
  
  public void close() { client.close(); }
}