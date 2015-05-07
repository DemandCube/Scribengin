package com.neverwinterdp.swing.registry;

import java.awt.event.ActionEvent;
import java.io.IOException;

import javax.swing.AbstractAction;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JToolBar;

import com.fasterxml.jackson.databind.JsonNode;
import com.neverwinterdp.registry.NodeInfo;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.util.text.DateUtil;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;
import com.neverwinterdp.util.JSONSerializer;

@SuppressWarnings("serial")
public class UIRegistryNodeTextView extends SpringLayoutGridJPanel {
  private String  path ;
  private JTextArea nodeDataTextArea ;
  private NodeInfoPanel nodeInfoPanel;
  
  public UIRegistryNodeTextView(String path) {
    this.path = path ;
    Registry registry = Cluster.getInstance().getRegistry();
    if(registry == null) {
      initNoConnection() ;
    } else {
      try {
        init(registry) ;
      } catch(Throwable e) {
        MessageUtil.handleError(e);
      }
    }
    makeCompactGrid(); 
  }

  private void initNoConnection() {
    JPanel infoPanel = new JPanel();
    infoPanel.add(new JLabel("No Registry Connection"));
    addRow(infoPanel);
  }
  
  private void init(Registry registry) throws RegistryException {
    JToolBar toolbar = new JToolBar();
    toolbar.setFloatable(false);
    toolbar.add(new AbstractAction("Reload") {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    });
    toolbar.add(new AbstractAction("Delete") {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    });
    toolbar.add(new AbstractAction("Watch Create") {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    });
    toolbar.add(new AbstractAction("Watch Modify") {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    });
    toolbar.add(new AbstractAction("Watch Delete") {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    });
    addRow(toolbar) ;

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