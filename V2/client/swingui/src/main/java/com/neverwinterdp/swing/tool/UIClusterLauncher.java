package com.neverwinterdp.swing.tool;

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JSeparator;
import javax.swing.SwingConstants;

import com.neverwinterdp.swing.tool.ClusterConfig.ZookeeperConfig;
import com.neverwinterdp.swing.widget.BeanBindingJTextField;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIClusterLauncher extends JPanel {
  private JPanel configPanels;
  private ClusterLauncherThread clusterLauncherThread ;
  
  public UIClusterLauncher() {
    ClusterConfig clusterConfig = Cluster.getInstance().getClusterConfig();
    configPanels = new JPanel(new CardLayout());
    configPanels.add(new ZookeeperConfigPanel(clusterConfig));
    configPanels.add(new KafkaConfigPanel(clusterConfig));
    configPanels.add(new VMConfigPanel(clusterConfig));
    configPanels.add(new ScribenginConfigPanel(clusterConfig));
    
    JButton nextBtn = new JButton("Next");
    nextBtn.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
        CardLayout layout = (CardLayout) (configPanels.getLayout());
        layout.next(configPanels);
      }
    });
    
    JButton prevBtn = new JButton("Previous");
    prevBtn.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
        CardLayout layout = (CardLayout) (configPanels.getLayout());
        layout.previous(configPanels);
      }
    });
    
    JButton launchBtn = new JButton("Launch");
    launchBtn.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
        launchCluster();
      }
    });
    
    SpringLayoutGridJPanel btnPanel = new SpringLayoutGridJPanel();
    btnPanel.addRow(prevBtn, nextBtn, launchBtn);
    btnPanel.makeGrid();
    
    setLayout(new BorderLayout());
    add(btnPanel, BorderLayout.NORTH);
    add(configPanels,   BorderLayout.CENTER);
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
        Cluster cluster = Cluster.getInstance();
        cluster.launch();
        join();
      } catch(Exception ex) {
        ex.printStackTrace();
      }
      System.out.println("ClusterLauncherThread finish run");
    }
  }
  
  static abstract public class ConfigPanel extends SpringLayoutGridJPanel {
    public ConfigPanel(ClusterConfig clusterConfig, String title, String description) {
      createBorder(title);
      addRow("<html>" + description + "</html>");
      addRow(new JSeparator(SwingConstants.HORIZONTAL));
      try {
        onInit(clusterConfig);
      } catch (Exception e) {
      }
      makeCompactGrid();
    }
    
    public void onInit(ClusterConfig clusterConfig) throws Exception {
    }
  }
  
  static  public class ZookeeperConfigPanel extends ConfigPanel {
    static String DESCRIPTION = "Zookeeper Configuration";
    
    public ZookeeperConfigPanel(ClusterConfig clusterConfig) {
      super(clusterConfig, "Zookeeper", DESCRIPTION);
    }
    
    public void onInit(ClusterConfig clusterConfig) throws Exception {
      SpringLayoutGridJPanel configPanel = new SpringLayoutGridJPanel() ;
      BeanBindingJTextField<ZookeeperConfig> numOfInstances = 
          new BeanBindingJTextField<>(clusterConfig.getZookeeperConfig(), "numOfInstances", true);
      configPanel.addRow("Num Of Instances", numOfInstances);
      configPanel.makeCompactGrid();
      
      addRow(configPanel);
    }
  }
  
  static  public class KafkaConfigPanel extends ConfigPanel {
    static String DESCRIPTION = "Kafka Configuration";
    
    public KafkaConfigPanel(ClusterConfig clusterConfig) {
      super(clusterConfig, "Kafka", DESCRIPTION);
    }
  }
  
  static  public class VMConfigPanel extends ConfigPanel {
    static String DESCRIPTION = "This configuration will launch the single vm master";
    
    public VMConfigPanel(ClusterConfig clusterConfig) {
      super(clusterConfig, "VM Configuration", DESCRIPTION);
    }
  }
  
  static  public class ScribenginConfigPanel extends ConfigPanel {
    static String DESCRIPTION =         
        "This configuration will launch the scribengin cluster " + 
        "that includes the vm master and scribengin master" ;
    
    public ScribenginConfigPanel(ClusterConfig clusterConfig) {
      super(clusterConfig, "Scribengin Cluster", DESCRIPTION);
    }
  }
  
  public static void main(String[] args) {
    JFrame frame = new JFrame("CardLayoutDemo");
    UIClusterLauncher changer = new UIClusterLauncher();
    frame.getContentPane().add(changer);
    frame.pack();
    frame.setVisible(true);
  }
}