package com.neverwinterdp.swing.tool;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;

import com.neverwinterdp.swing.tool.EmbeddedClusterConfig.KafkaConfig;
import com.neverwinterdp.swing.tool.EmbeddedClusterConfig.ZookeeperConfig;
import com.neverwinterdp.swing.widget.BeanBindingJTextField;
import com.neverwinterdp.swing.widget.GridLayoutPanel;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIEmbeddedCluster extends JPanel {
  private ClusterLauncherThread clusterLauncherThread ;
  
  private  EmbeddedClusterConfig embeddedClusterConfig  = new  EmbeddedClusterConfig();
  
  public UIEmbeddedCluster() {
    SpringLayoutGridJPanel configPanels = new SpringLayoutGridJPanel();
    configPanels.addRow(new ZookeeperConfigPanel(embeddedClusterConfig));
    configPanels.addRow(new KafkaConfigPanel(embeddedClusterConfig));
    configPanels.makeGrid();
    
    JButton connectBtn = new JButton("Connect");
    connectBtn.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    });
    
    JButton disconnectBtn = new JButton("Disconnect");
    disconnectBtn.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    });
    
    
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
    
    GridLayoutPanel btnPanel = new GridLayoutPanel();
    btnPanel.addCells(startScribenginBtn, shutdownScribenginBtn, connectBtn, disconnectBtn);
    btnPanel.makeGrid(2);
    
    setLayout(new BorderLayout());
    add(configPanels,   BorderLayout.NORTH);
    add(btnPanel, BorderLayout.CENTER);
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
        EmbeddedCluster cluster = new EmbeddedCluster(embeddedClusterConfig);
        Cluster.setCurrentInstance(cluster);
        cluster.startDependencySevers();
        cluster.startVMMaster();
        cluster.startScribenginMaster();
        cluster.runKafkaToKafkaDataflow();
      } catch(Exception ex) {
        ex.printStackTrace();
      }
      System.out.println("ClusterLauncherThread finish run");
    }
  }
  
  static  public class ZookeeperConfigPanel extends SpringLayoutGridJPanel {
    static String DESCRIPTION = "Zookeeper Configuration";
    
    public ZookeeperConfigPanel(EmbeddedClusterConfig embeddedClusterConfig) {
      createBorder("Zookeeper Cluster Configuration");
      ZookeeperConfig zkConfig = embeddedClusterConfig.getZookeeperConfig();
      addRow("Num Of Instances", new BeanBindingJTextField<>(zkConfig, "numOfInstances", true));
      addRow("Start Port", new BeanBindingJTextField<>(zkConfig, "startPort", true));
      makeCompactGrid();
    }
  }
  
  static  public class KafkaConfigPanel extends SpringLayoutGridJPanel {
    public KafkaConfigPanel(EmbeddedClusterConfig embeddedClusterConfig) {
      createBorder("Kafka Cluster Configuration");
      KafkaConfig kafkaConfig = embeddedClusterConfig.getKafkaConfig();
      addRow("Num Of Instances", new BeanBindingJTextField<>(kafkaConfig, "numOfInstances", true));
      addRow("Start Port", new BeanBindingJTextField<>(kafkaConfig, "startPort", true));
      makeCompactGrid();
    }
  }
  
  public static void main(String[] args) {
    JFrame frame = new JFrame("CardLayoutDemo");
    UIEmbeddedCluster changer = new UIEmbeddedCluster();
    frame.getContentPane().add(changer);
    frame.pack();
    frame.setVisible(true);
  }
}