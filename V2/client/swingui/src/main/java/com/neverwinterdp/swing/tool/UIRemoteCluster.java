package com.neverwinterdp.swing.tool;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JPanel;

import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.swing.widget.BeanBindingJComboBox;
import com.neverwinterdp.swing.widget.GridLayoutPanel;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIRemoteCluster extends JPanel {
  static private String[]  REMOTE_ZKS = {
    "test.scribegnin:2181", "slave3:2181"
  };
  
  private RegistryConfig registryConfig = RegistryConfig.getDefault();
  
  public UIRemoteCluster() {
    setLayout(new BorderLayout());

    SpringLayoutGridJPanel configPanel = new SpringLayoutGridJPanel();
    configPanel.createBorder("Remote Zookeeper Configuration");
    
    BeanBindingJComboBox<RegistryConfig, String> selector = 
        new BeanBindingJComboBox<>(registryConfig, "connect", REMOTE_ZKS);
    selector.setEditable(true);
    configPanel.addRow(selector);
    configPanel.makeCompactGrid();
    
    Action connectBtn = new AbstractAction("Connect") {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    };
    Action disconnectBtn = new AbstractAction("Disconnect") {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    };
    
    JPanel btnPanel = new JPanel();
    btnPanel.add(new JButton(connectBtn));
    btnPanel.add(new JButton(disconnectBtn));
    
    add(configPanel, BorderLayout.NORTH);
    add(btnPanel, BorderLayout.CENTER);
  }
}
