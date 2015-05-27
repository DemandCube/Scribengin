package com.neverwinterdp.swing.es;

import java.awt.BorderLayout;
import java.util.Map;

import javax.swing.JPanel;
import javax.swing.JScrollPane;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.Settings;

import com.neverwinterdp.swing.widget.JTabbedPaneUI;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

public class UIClusterInfo extends JPanel {
  private ClusterState clusterState;
  private JTabbedPaneUI tabbedPaneUI ;
  
  public UIClusterInfo(ClusterState clusterState) {
    this.clusterState = clusterState ;
    setLayout(new BorderLayout());
    tabbedPaneUI = new JTabbedPaneUI() ;
    tabbedPaneUI.addTab("Settings", new JScrollPane(new SettingsPanel(clusterState)), false);
    tabbedPaneUI.addTab("Routing Table", new JScrollPane(new RoutingTablePanel(clusterState)), false);
    add(tabbedPaneUI, BorderLayout.CENTER);
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
  
  @SuppressWarnings("serial")
  static public class SettingsPanel extends SpringLayoutGridJPanel {
    public SettingsPanel(ClusterState clusterState) {
      onRefresh(clusterState);
    }
    
    public void onRefresh(ClusterState clusterState) {
      Settings settings = clusterState.getMetaData().settings() ;
      ImmutableMap<String, String> map = settings.getAsMap();
      addRow("Name", "Value");
      for(Map.Entry<String, String> entry : map.entrySet()) {
        addRow(entry.getKey(), entry.getValue());
      }
      makeGrid(); 
    }
  }
}
