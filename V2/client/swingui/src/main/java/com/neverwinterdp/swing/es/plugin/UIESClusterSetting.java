package com.neverwinterdp.swing.es.plugin;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JPanel;

import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.es.ESCluster;
import com.neverwinterdp.swing.es.ESClusterConfiguration;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.BeanBindingJComboBox;
import com.neverwinterdp.swing.widget.BeanBindingJTextField;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIESClusterSetting extends JPanel  implements UILifecycle {
  public UIESClusterSetting() {
    setLayout(new BorderLayout()) ;
  }
  
  @Override
  public void onInit() throws Exception {
  }

  @Override
  public void onDestroy() throws Exception {
  }

  @Override
  public void onActivate() {
    removeAll();
    ElasticSearchConfigPanel configPanel = new ElasticSearchConfigPanel() ;
    add(configPanel, BorderLayout.CENTER);
    
    ESClusterConnectionPanel connectionPanel = new ESClusterConnectionPanel();
    add(connectionPanel, BorderLayout.SOUTH);
  }

  @Override
  public void onDeactivate() throws Exception {
    removeAll();
  }
  
  static  public class ESClusterConnectionPanel extends SpringLayoutGridJPanel {
    static private String[]  REMOTE_ES = {
      "test.scribengin:9300"
    };
    
    private String connect;
    
    public ESClusterConnectionPanel() {
      createBorder("ElasticSearch Remote Conenction");
      BeanBindingJComboBox<ESClusterConnectionPanel, String> selector = new BeanBindingJComboBox<>(this, "connect", REMOTE_ES);
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
    
    public String getConnect() { return this.connect; }
    public void   setConnect(String connect) { this.connect = connect; }
    
    private void onConnect() {
      try {
        System.out.println("Connecting to the remote elasticsearch " + connect);
        ESCluster.getInstance().connect(connect);
        System.out.println("Connected to the remote elasticsearch " + connect);
      } catch (Exception error) {
        MessageUtil.handleError(error);
      }
    }
    
    private void onDisconnect() {
      try {
        ESCluster.getInstance().disconnect();
      } catch (Throwable error) {
        MessageUtil.handleError(error);
      }
    }
  }
  
  
  public class ElasticSearchConfigPanel extends SpringLayoutGridJPanel {
    private ESClusterConfiguration configuration = new ESClusterConfiguration() ;
    
    public ElasticSearchConfigPanel() {
      createBorder("Embedded ElasticSearch Configuration");
      
      BeanBindingJTextField<ESClusterConfiguration> numOfInstances = new BeanBindingJTextField<>(configuration, "numOfInstances"); 
      numOfInstances.setTypeConverter(BeanBindingJTextField.INTERGER);
      addRow("Num Of Instances");
      addRow(numOfInstances);
      
      BeanBindingJTextField<ESClusterConfiguration> basePort = new BeanBindingJTextField<>(configuration, "basePort");
      basePort.setTypeConverter(BeanBindingJTextField.INTERGER);
      addRow("Start Port");
      addRow(new BeanBindingJTextField<>(configuration, "basePort"));
      
      addRow("Base Dir");
      addRow(new BeanBindingJTextField<>(configuration, "baseDir"));
      
      Action connectBtn = new AbstractAction("Start") {
        @Override
        public void actionPerformed(ActionEvent e) {
          try {
            ESCluster.getInstance().startEmbeddedCluster(configuration);
          } catch (Exception ex) {
            MessageUtil.handleError(ex);
          }
        }
      };
      Action disconnectBtn = new AbstractAction("Shutdown") {
        @Override
        public void actionPerformed(ActionEvent e) {
          try {
            ESCluster.getInstance().shutdownEmbeddedCluster();
          } catch (Exception ex) {
            MessageUtil.handleError(ex);
          }
        }
      };
      
      JPanel btnPanel = new JPanel();
      btnPanel.add(new JButton(connectBtn));
      btnPanel.add(new JButton(disconnectBtn));
      
      addRow(btnPanel);
      makeCompactGrid();
    }
  }
}