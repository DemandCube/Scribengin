package com.neverwinterdp.swing.tool;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JPanel;

import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.BeanBindingJComboBox;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIRemoteCluster extends JPanel {
  static private String[]  REMOTE_ZKS = {
    "test.scribengin:2181", "128.199.113.190:2181"
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
        onConnect();
      }
    };
    Action disconnectBtn = new AbstractAction("Disconnect") {
      @Override
      public void actionPerformed(ActionEvent e) {
        onDisconnect();
      }
    };
    
    JPanel btnPanel = new JPanel();
    btnPanel.add(new JButton(connectBtn));
    btnPanel.add(new JButton(disconnectBtn));
    
    add(configPanel, BorderLayout.NORTH);
    add(btnPanel, BorderLayout.CENTER);
  }
  
  private void onConnect() {
    try {
      System.out.println("Connecting to the remote zookeeper " + registryConfig.getConnect());
      RemoteCluster cluster = new RemoteCluster() ;
      cluster.connect(registryConfig);
      Cluster.setCurrentInstance(cluster);
      System.out.println("Connected to the remote zookeeper " + registryConfig.getConnect());
    } catch (Exception error) {
      MessageUtil.handleError(error);
    }
  }
  
  private void onDisconnect() {
    try {
      RemoteCluster cluster = (RemoteCluster)Cluster.getCurrentInstance();
      cluster.disconnect();
    } catch (Throwable error) {
      MessageUtil.handleError(error);
    }
  }
}
