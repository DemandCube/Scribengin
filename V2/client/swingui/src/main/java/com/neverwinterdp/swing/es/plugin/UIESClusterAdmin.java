package com.neverwinterdp.swing.es.plugin;

import java.awt.BorderLayout;
import java.util.Iterator;
import java.util.Map;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;

import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.UIMain;
import com.neverwinterdp.swing.UIWorkspace;
import com.neverwinterdp.swing.es.ESCluster;
import com.neverwinterdp.swing.es.UIClusterInfo;
import com.neverwinterdp.swing.es.UIIndexInfo;
import com.neverwinterdp.swing.util.SwingUtil;

@SuppressWarnings("serial")
public class UIESClusterAdmin extends JPanel implements UILifecycle {
  public UIESClusterAdmin() {
    setLayout(new BorderLayout());
  }

  @Override
  public void onInit() throws Exception {
  }

  @Override
  public void onDestroy() throws Exception {
  }

  @Override
  public void onActivate() throws Exception {
    removeAll();
    
    NavigationNode navigationNode = new NavigationNode();
   
    navigationNode.setShowsRootHandles(true);
    add(new JScrollPane(navigationNode), BorderLayout.CENTER);
  }

  @Override
  public void onDeactivate() throws Exception {
    removeAll();
  }
  
  public class NavigationNode extends JTree {
    public NavigationNode() throws Exception {
      addTreeSelectionListener(new  TreeSelectionListener() {
        public void valueChanged(TreeSelectionEvent evt) {
          DefaultMutableTreeNode node = (DefaultMutableTreeNode) NavigationNode.this.getLastSelectedPathComponent();
          if (node == null) return;
          onSelect(node) ;
        }
      }) ;
      setModel(new DefaultTreeModel(new ClusterNode()));
    }
    
    public void onSelect(DefaultMutableTreeNode node) {
      SelectableNode selectableNode = (SelectableNode)node;
      UIMain uiScribengin = SwingUtil.findAncestorOfType(UIESClusterAdmin.this, UIMain.class) ;
      UIWorkspace uiWorkspace = uiScribengin.getUiWorkspace();
      selectableNode.onSelect(uiWorkspace);
    }
  }
  
  static public class SelectableNode extends DefaultMutableTreeNode {
    public void onSelect(UIWorkspace uiWorkspace) {
      System.out.println("On select node: " + getClass());
    }
  }
  
  public class ClusterNode extends SelectableNode {
    private static final long serialVersionUID = 1L;
    
    private ClusterState clusterState;
    
    public ClusterNode() {
      setUserObject("ElasticSearch Cluster") ;
      ESCluster esCluster = ESCluster.getInstance();
      ESClient esClient = esCluster.getESClient();
      clusterState = esClient.getClusterState();
      
      add(new ServersNode(clusterState));
      add(new IndicesNode(esClient.getIndexStats()));
    }
    
    public void onSelect(UIWorkspace uiWorkspace) {
      uiWorkspace.addTab("ElasticSearch Cluster Info", new UIClusterInfo(clusterState), true);
    }
  }
  
  public class ServersNode extends SelectableNode {
    ClusterState clusterState;
    
    public ServersNode(ClusterState clusterState) {
      setUserObject("Servers") ;
      this.clusterState = clusterState;
      DiscoveryNodes nodes = clusterState.getNodes() ;
      Iterator<String> keyIterator = nodes.getNodes().keysIt();
      while(keyIterator.hasNext()) {
        String nodeId  = keyIterator.next();
        add(new ServerNode(nodes.get(nodeId)));
      }
    }
    
    public void onSelect(UIWorkspace uiWorkspace) {
    }
  }
  
  public class ServerNode extends SelectableNode {
    private static final long serialVersionUID = 1L;
    private DiscoveryNode discoveryNode ;
    
    public ServerNode(DiscoveryNode discoveryNode) {
      setUserObject(discoveryNode.address().toString()) ;
      this.discoveryNode = discoveryNode;
    }

  }
  
  public class IndicesNode extends SelectableNode {
    Map<String, IndexStats> indicesStats;
    
    public IndicesNode( Map<String, IndexStats> indicesStats) {
      setUserObject("Indices") ;
      this.indicesStats = indicesStats;
      for(IndexStats  indexStats : indicesStats.values()) {
        add(new IndexNode(indexStats));
      }
    }
  }
  
  public class IndexNode extends SelectableNode {
    private static final long serialVersionUID = 1L;
    String index;
    
    public IndexNode(IndexStats indexStats) {
      index = indexStats.getIndex();
      setUserObject(index);
      Map<Integer, IndexShardStats> shardsStats = indexStats.getIndexShards() ;
      for(IndexShardStats shardStats : shardsStats.values()) {
        add(new ShardNode(shardStats));
      }
    }

    public void onSelect(UIWorkspace uiWorkspace) {
      uiWorkspace.addTab("ES Index " + index, new UIIndexInfo(index), true);
    }
  }
  
  public class ShardNode extends SelectableNode {
    private static final long serialVersionUID = 1L;
    
    private IndexShardStats indexShardStats;
    
    public ShardNode(IndexShardStats indexShardStats) {
      setUserObject(indexShardStats.getShardId().getId());
    }
  }  
}