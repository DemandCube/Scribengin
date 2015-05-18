package com.neverwinterdp.swing.tool;

import java.awt.FlowLayout;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JPanel;

import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.scribengin.dataflow.test.DataflowTestRunner;

@SuppressWarnings("serial")
public class UIDataflowTests extends JPanel implements UILifecycle {
  public UIDataflowTests() {
    setLayout(new FlowLayout()) ;
  }
  
  @Override
  public void onInit() throws Exception {
  }

  @Override
  public void onDestroy() throws Exception {
  }

  @Override
  public void onActivate() throws Exception {
    removeAll();
    Action kafkaToKafkaTest = new AbstractAction("Kafka To Kafka Dataflow Test") {
      @Override
      public void actionPerformed(ActionEvent e) {
        new DataflowTestRunner.KafkaToKakaDataflowTestRunner().start();
      }
    };
    
    Action hdfsToHdfsTest = new AbstractAction("Hdfs To Hdfs Dataflow Test") {
      @Override
      public void actionPerformed(ActionEvent e) {
        new DataflowTestRunner.HDFSToHDFSDataflowTestRunner().start();
      }
    };
    
    Action dataflowServerFailureTest = new AbstractAction("Server Failure Dataflow Test") {
      @Override
      public void actionPerformed(ActionEvent e) {
        new DataflowTestRunner.ServerFailureDataflowTestRunner().start();
      }
    };
    
    add(new JButton(kafkaToKafkaTest));
    add(new JButton(hdfsToHdfsTest));
    add(new JButton(dataflowServerFailureTest));
  }

  @Override
  public void onDeactivate() throws Exception {
    removeAll();
  }
}
