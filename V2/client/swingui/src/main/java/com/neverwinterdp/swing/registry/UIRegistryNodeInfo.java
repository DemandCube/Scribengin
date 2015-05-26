package com.neverwinterdp.swing.registry;

import java.awt.event.ActionEvent;
import java.io.IOException;

import javax.swing.AbstractAction;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JToolBar;

import com.fasterxml.jackson.databind.JsonNode;
import com.neverwinterdp.registry.NodeInfo;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.scribengin.ScribenginCluster;
import com.neverwinterdp.swing.util.text.DateUtil;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;
import com.neverwinterdp.util.JSONSerializer;

@SuppressWarnings("serial")
public class UIRegistryNodeInfo extends SpringLayoutGridJPanel implements UILifecycle {
  private String  path ;
  private JTextArea nodeDataTextArea ;
  private NodeInfoPanel nodeInfoPanel;
  
  public UIRegistryNodeInfo(String path) {
    this.path = path ;
  }

  @Override
  public void onInit() throws Exception {
  }

  @Override
  public void onDestroy() throws Exception {
  }

  @Override
  public void onActivate() throws Exception {
    clear();
    Registry registry = ScribenginCluster.getCurrentInstance().getRegistry();
    if(registry == null) {
      addRow("No Registry Connection");
    } else {
      refresh(registry) ;
    }
    makeCompactGrid(); 
  }

  @Override
  public void onDeactivate() throws Exception {
    clear();
  }
  
  private void refresh(Registry registry) throws RegistryException {
    NodeActionToolBar toolbar = new NodeActionToolBar();
    addRow(toolbar);
    
    NodeInfo nodeInfo = registry.getInfo(path);
    nodeInfoPanel = new NodeInfoPanel(path, nodeInfo);
    addRow(nodeInfoPanel);
    
    nodeDataTextArea = new JTextArea();
    nodeDataTextArea.setLineWrap(true);
    nodeDataTextArea.setEditable(false);
    nodeDataTextArea.setVisible(true);

    byte[] data = registry.getData(path);
    String text  = "" ;
    if(data != null && data.length > 0) {
      try {
        text  = new String(data) ;
        JsonNode jsonNode = JSONSerializer.INSTANCE.fromString(text) ;
        text = JSONSerializer.INSTANCE.toString(jsonNode);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    nodeDataTextArea.setText(text);
    
    JScrollPane scrollNodeDataTextArea = new JScrollPane(nodeDataTextArea);
    scrollNodeDataTextArea.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
    scrollNodeDataTextArea.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
    addRow(scrollNodeDataTextArea);
  }
  
  static public class NodeActionToolBar extends JToolBar {
    public NodeActionToolBar() {
      setFloatable(false);
      add(new AbstractAction("Reload") {
        @Override
        public void actionPerformed(ActionEvent e) {
        }
      });
      add(new AbstractAction("Delete") {
        @Override
        public void actionPerformed(ActionEvent e) {
        }
      });
      add(new AbstractAction("Watch Create") {
        @Override
        public void actionPerformed(ActionEvent e) {
        }
      });
      add(new AbstractAction("Watch Modify") {
        @Override
        public void actionPerformed(ActionEvent e) {
        }
      });
      add(new AbstractAction("Watch Delete") {
        @Override
        public void actionPerformed(ActionEvent e) {
        }
      });
    }
  }
  
  static public class NodeInfoPanel extends SpringLayoutGridJPanel {
    public NodeInfoPanel(String path, NodeInfo nodeInfo) {
      update(path, nodeInfo);
    }
    
    public void update(String path, NodeInfo nodeInfo) {
      createBorder("Node Info");
      addRow("Path         : ", path);
      addRow("Created Time : ", DateUtil.COMPACT_DATE_TIME.format(nodeInfo.getCtime()));
      addRow("Modified Time: ", DateUtil.COMPACT_DATE_TIME.format(nodeInfo.getMtime()));
      addRow("Version      : ", nodeInfo.getVersion());
      addRow("Data Length  : ", nodeInfo.getDataLength());
      addRow("Children Size: ", nodeInfo.getNumOfChildren());
      makeCompactGrid(); 
    }
  }
}