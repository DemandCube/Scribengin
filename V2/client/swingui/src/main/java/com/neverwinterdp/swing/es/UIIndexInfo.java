package com.neverwinterdp.swing.es;

import java.awt.BorderLayout;
import java.util.Map;

import javax.swing.JPanel;
import javax.swing.JScrollPane;

import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.indexing.IndexingStats.Stats;
import org.elasticsearch.index.shard.DocsStats;

import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.JTabbedPaneUI;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIIndexInfo extends JPanel {
  private JTabbedPaneUI tabbedPaneUI ;
  
  public UIIndexInfo(String index)  {
    setLayout(new BorderLayout());
    try {
      ESClient esClient = ESCluster.getInstance().getESClient();
      ClusterState clusterState = esClient.getClusterState();
      IndexStats indexStats = esClient.getIndexStats().get(index) ;
      tabbedPaneUI = new JTabbedPaneUI() ;
      tabbedPaneUI.addTab("Primaries", new JScrollPane(new PrimariesPanel(indexStats)), false);
      tabbedPaneUI.addTab("Routing Table", new JScrollPane(new RoutingTablePanel(clusterState)), false);
      tabbedPaneUI.addTab("Search", new UISearchQuery(indexStats.getIndex(), new UISearchQueryLog4jPlugin()), false);
      tabbedPaneUI.setSelectedTab(0);
      add(tabbedPaneUI, BorderLayout.CENTER);
    } catch(Exception ex) {
      MessageUtil.handleError(ex);
    }
  }
  
  @SuppressWarnings("serial")
  static public class PrimariesPanel extends SpringLayoutGridJPanel {
    public PrimariesPanel(IndexStats indexStats) {
      onRefresh(indexStats);
    }
    
    public void onRefresh(IndexStats indexStats) {
      CommonStats primaries = indexStats.getPrimaries() ;
      DocsStats docs = primaries.getDocs();
      addRow("Docs", "", "") ;
      addRow("", "Count", docs.getCount()) ;
      addRow("", "Deleted", docs.getDeleted()) ;
      IndexingStats indexingStats = primaries.getIndexing() ;
      addRow("Indexing", "", "") ;
      Stats totalStats = indexingStats.getTotal();
      addRow("", "Count", totalStats.getIndexCount()) ;
      addRow("", "Current", totalStats.getIndexCurrent()) ;
      addRow("", "Index Time", totalStats.getIndexTimeInMillis() + "ms") ;
      addRow("", "Delete", totalStats.getDeleteCount()) ;
      addRow("", "Delete Current", totalStats.getDeleteCurrent()) ;
      addRow("", "Delete Time", totalStats.getDeleteTimeInMillis() + "ms") ;

      makeGrid();
    }
  }
  
  @SuppressWarnings("serial")
  static public class RoutingTablePanel extends SpringLayoutGridJPanel {
    public RoutingTablePanel(ClusterState clusterState) {
      onRefresh(clusterState);
    }
    
    public void onRefresh(ClusterState clusterState) {
      RoutingTable routingTable = clusterState.getRoutingTable() ;
      Map<String, IndexRoutingTable> indicesRountingTable = routingTable.getIndicesRouting() ;
      for(IndexRoutingTable sel : indicesRountingTable.values()) {
        addRow(sel.getIndex(), "", "");
      }
      makeGrid();
    }
  }
}
