package com.neverwinterdp.swing.es;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JPanel;

import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.BeanBindingJComboBox;
import com.neverwinterdp.swing.widget.BeanBindingJTextField;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIESCluster extends SpringLayoutGridJPanel  implements UILifecycle {
  public UIESCluster() {
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
    ElasticSearchConfigPanel configPanel = new ElasticSearchConfigPanel() ;
    addRow(configPanel);
    
    ESClusterConnectionPanel connectionPanel = new ESClusterConnectionPanel();
    addRow(connectionPanel);
    makeCompactGrid();
  }

  @Override
  public void onDeactivate() throws Exception {
    clear();
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
  
  
  public class ElasticSearchConfigPanel extends JPanel {
    private ESClusterConfiguration configuration = new ESClusterConfiguration() ;
    
    public ElasticSearchConfigPanel() {
      setLayout(new BorderLayout());
      
      SpringLayoutGridJPanel configPanel = new SpringLayoutGridJPanel() ;
      configPanel.createBorder("ElasticSearch Configuration");
      configPanel.addRow("Num Of Instances", new BeanBindingJTextField<>(configuration, "numOfInstances", true));
      configPanel.addRow("Start Port", new BeanBindingJTextField<>(configuration, "basePort", true));
      configPanel.makeCompactGrid();
      
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
      
      add(configPanel, BorderLayout.NORTH);
      add(btnPanel, BorderLayout.CENTER);
    }
  }
}