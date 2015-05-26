package com.neverwinterdp.swing.scribengin;

import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JPanel;

import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.scribengin.ScribenginEmbeddedClusterConfig.KafkaConfig;
import com.neverwinterdp.swing.scribengin.ScribenginEmbeddedClusterConfig.ZookeeperConfig;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.BeanBindingJComboBox;
import com.neverwinterdp.swing.widget.BeanBindingJTextField;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIScribenginCluster extends SpringLayoutGridJPanel  implements UILifecycle {
  private ClusterLauncherThread clusterLauncherThread ;
  
  private  ScribenginEmbeddedClusterConfig scribenginEmbeddedClusterConfig  = new  ScribenginEmbeddedClusterConfig();
  
  public UIScribenginCluster() {
  }
  
  @Override
  public void onInit() throws Exception {
  }

  @Override
  public void onDestroy() throws Exception {
  }

  @Override
  public void onActivate() {
    clear();
    addRow(new ZookeeperConfigPanel(scribenginEmbeddedClusterConfig));
    addRow(new KafkaConfigPanel(scribenginEmbeddedClusterConfig));
    
    JButton startScribenginBtn = new JButton("Start");
    startScribenginBtn.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
        launchCluster();
      }
    });
    
    JButton shutdownScribenginBtn = new JButton("Shutdown");
    startScribenginBtn.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    });
    
    JPanel btnPanel = new JPanel(new FlowLayout());
    btnPanel.add(startScribenginBtn);
    btnPanel.add(shutdownScribenginBtn);
    
    addRow(btnPanel);
    
    ZookeeperConnectionPanel remoteClusterPanel = new ZookeeperConnectionPanel() ;
    addRow(remoteClusterPanel);
    
    makeCompactGrid();
  }

  @Override
  public void onDeactivate() throws Exception {
    clear();
  }

  void launchCluster() {
    if(clusterLauncherThread != null) {
      System.err.println("clusterLauncherThread is not null") ;
      return ;
    }
    clusterLauncherThread = new ClusterLauncherThread() ;
    clusterLauncherThread.start();
  }
  
  public class ClusterLauncherThread extends Thread {
    public void run() {
      try {
        ScribenginEmbeddedCluster cluster = new ScribenginEmbeddedCluster(scribenginEmbeddedClusterConfig);
        ScribenginCluster.setCurrentInstance(cluster);
        cluster.startDependencySevers();
        cluster.startVMMaster();
        cluster.startScribenginMaster();
      } catch(Exception ex) {
        ex.printStackTrace();
      }
      System.out.println("ClusterLauncherThread finish run");
    }
  }
  
  static  public class ZookeeperConfigPanel extends SpringLayoutGridJPanel {
    public ZookeeperConfigPanel(ScribenginEmbeddedClusterConfig scribenginEmbeddedClusterConfig) {
      createBorder("Zookeeper Configuration");
      ZookeeperConfig zkConfig = scribenginEmbeddedClusterConfig.getZookeeperConfig();
      addRow("Num Of Instances", new BeanBindingJTextField<>(zkConfig, "numOfInstances", true));
      addRow("Start Port", new BeanBindingJTextField<>(zkConfig, "startPort", true));
      makeCompactGrid();
    }
  }
  
  static  public class KafkaConfigPanel extends SpringLayoutGridJPanel {
    public KafkaConfigPanel(ScribenginEmbeddedClusterConfig scribenginEmbeddedClusterConfig) {
      createBorder("Kafka Cluster Configuration");
      KafkaConfig kafkaConfig = scribenginEmbeddedClusterConfig.getKafkaConfig();
      addRow("Num Of Instances", new BeanBindingJTextField<>(kafkaConfig, "numOfInstances", true));
      addRow("Start Port", new BeanBindingJTextField<>(kafkaConfig, "startPort", true));
      makeCompactGrid();
    }
  }
  
  static  public class ZookeeperConnectionPanel extends SpringLayoutGridJPanel {
    static private String[]  REMOTE_ZKS = {
      "test.scribengin:2181"
    };
    
    private RegistryConfig registryConfig = RegistryConfig.getDefault();
    
    public ZookeeperConnectionPanel() {
      createBorder("Zookeeper Remote Conenction");
      BeanBindingJComboBox<RegistryConfig, String> selector = 
          new BeanBindingJComboBox<>(registryConfig, "connect", REMOTE_ZKS);
      selector.setEditable(true);
      selector.setSelectedIndex(0);
      addRow(selector);
      
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
      
      addRow(btnPanel);
      makeCompactGrid();
    }
    
    private void onConnect() {
      try {
        System.out.println("Connecting to the remote zookeeper " + registryConfig.getConnect());
        ScribenginRemoteCluster cluster = new ScribenginRemoteCluster() ;
        cluster.connect(registryConfig);
        ScribenginCluster.setCurrentInstance(cluster);
        System.out.println("Connected to the remote zookeeper " + registryConfig.getConnect());
      } catch (Exception error) {
        MessageUtil.handleError(error);
      }
    }
    
    private void onDisconnect() {
      try {
        ScribenginRemoteCluster cluster = (ScribenginRemoteCluster)ScribenginCluster.getCurrentInstance();
        cluster.disconnect();
      } catch (Throwable error) {
        MessageUtil.handleError(error);
      }
    }
  }
  
}