package com.neverwinterdp.swing.es.plugin;

import java.awt.BorderLayout;
import java.util.Iterator;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

import org.elasticsearch.action.admin.indices.status.IndexStatus;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;

import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.es.ESCluster;
import com.neverwinterdp.swing.widget.LazyLoadJTree;
import com.neverwinterdp.swing.widget.LazyLoadJTree.LazyLoadTreeNode;

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
    ESCluster esCluster = ESCluster.getInstance();
    ESClient esClient = esCluster.getESClient();
    ClusterState clusterState = esClient.getClusterState();
    DiscoveryNodes nodes = clusterState.getNodes() ;

    DefaultMutableTreeNode root = new DefaultMutableTreeNode("/");
    DefaultMutableTreeNode servers = new DefaultMutableTreeNode("servers");
    root.add(servers);
    Iterator<String> keyIterator = nodes.getNodes().keysIt();
    while(keyIterator.hasNext()) {
      servers.add(new ServerNode(keyIterator.next()));
    }
    
    DefaultMutableTreeNode indices = new DefaultMutableTreeNode("indices");
    for(IndexStatus sel : esClient.getIndexStatus()) {
      indices.add(new IndexNode(sel.getIndex()));
    }
    root.add(indices);
    
    DefaultTreeModel model = new DefaultTreeModel(root);
    RegistryNodeSelector registryTree = new RegistryNodeSelector(model);
    registryTree.setShowsRootHandles(true);
    add(new JScrollPane(registryTree), BorderLayout.CENTER);
  }

  @Override
  public void onDeactivate() throws Exception {
    removeAll();
  }
  
  public class RegistryNodeSelector extends JTree {
    public RegistryNodeSelector(final DefaultTreeModel model) throws Exception {
      super(model);
    }
    
    public void onSelect(DefaultMutableTreeNode node) {
    }
  }
  
  public class ServerNode extends DefaultMutableTreeNode {
    private static final long serialVersionUID = 1L;
    private String nodeId;
    
    public ServerNode(String id) {
      super(id) ;
      this.nodeId = id;
    }

    public String getNodeId() { return this.nodeId ; }
  }
  
  public class IndexNode extends DefaultMutableTreeNode {
    private static final long serialVersionUID = 1L;
    
    String name ;
    
    public IndexNode(String name) {
      super(name);
      this.name = name ;
    }

    public String getName() { return this.name ; }
  }
}
