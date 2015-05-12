package com.neverwinterdp.swing.tool;

import java.awt.FlowLayout;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JPanel;

import com.neverwinterdp.swing.util.MessageUtil;

@SuppressWarnings("serial")
public class UITests extends JPanel {
  public UITests() {
    setLayout(new FlowLayout()) ;
    
    Action action = new AbstractAction("Kafka To Kafka Dataflow Test") {
      @Override
      public void actionPerformed(ActionEvent e) {
        new TestRunnerThread().start();
      }
    };
    add(new JButton(action));
  }
  
  static public class TestRunnerThread extends Thread {
    public void run() {
      try {
        Cluster cluster = Cluster.getCurrentInstance() ;
        cluster.runKafkaToKafkaDataflow();
      } catch (Exception e) {
        e.printStackTrace();
        MessageUtil.handleError(e);
      }
    }
  }
}
