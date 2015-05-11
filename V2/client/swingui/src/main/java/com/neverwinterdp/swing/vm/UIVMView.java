package com.neverwinterdp.swing.vm;

import java.awt.event.ActionEvent;
import java.io.IOException;

import javax.swing.AbstractAction;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JToolBar;

import com.fasterxml.jackson.databind.JsonNode;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;
import com.neverwinterdp.util.JSONSerializer;

@SuppressWarnings("serial")
public class UIVMView extends SpringLayoutGridJPanel {
  private String  vmId ;
  private JTextArea nodeDataTextArea ;
  private NodeInfoPanel nodeInfoPanel;
  
  public UIVMView(String vmId) {
    this.vmId = vmId ;
    Registry registry = Cluster.getCurrentInstance().getRegistry();
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
    toolbar.add(new AbstractAction("Shutdown") {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    });
    addRow(toolbar) ;

    Node vmNode = registry.get("/vm/instances/all/" + vmId);
    
    nodeInfoPanel = new NodeInfoPanel(vmNode);
    addRow(nodeInfoPanel);
    
    nodeDataTextArea = new JTextArea();
    nodeDataTextArea.setLineWrap(true);
    nodeDataTextArea.setEditable(false);
    nodeDataTextArea.setVisible(true);

    String text  = vmNode.getDataAsString();
    if(text.length() > 0) {
      try {
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
    public NodeInfoPanel(Node vmNode) throws RegistryException {
      update(vmNode);
    }
    
    public void update(Node vmNode) throws RegistryException {
      createBorder("Node Info");
      addRow("Path        : ", vmNode.getPath());
      addRow("VM Status   : ", new String(vmNode.getChild("status").getDataAsString()));
      addRow("VM Heartbeat: ", vmNode.getDescendant("status/heartbeat").exists());
      makeCompactGrid(); 
    }
  }
}