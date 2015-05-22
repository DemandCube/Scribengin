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
    Action kafkaToKafkaTest = new AbstractAction("Dataflow Kafka To Kafka Test") {
      @Override
      public void actionPerformed(ActionEvent e) {
        new DataflowTestRunner.DataflowKafkaToKakaTestRunner().start();
      }
    };
    
    Action hdfsToHdfsTest = new AbstractAction("Dataflow Hdfs To Hdfs Test") {
      @Override
      public void actionPerformed(ActionEvent e) {
        new DataflowTestRunner.DataflowHDFSToHDFSTestRunner().start();
      }
    };
    
    Action dataflowServerFailureTest = new AbstractAction("Dataflow Worker Failure Test") {
      @Override
      public void actionPerformed(ActionEvent e) {
        new DataflowTestRunner.DataflowWorkerFailureTestRunner().start();
      }
    };
    
    Action dataflowStartStopResumeTest = new AbstractAction("Dataflow Start/Stop/Resume Test") {
      @Override
      public void actionPerformed(ActionEvent e) {
        new DataflowTestRunner.DataflowStartStopResumtTestRunner().start();
      }
    };
    
    add(new JButton(kafkaToKafkaTest));
    add(new JButton(hdfsToHdfsTest));
    add(new JButton(dataflowServerFailureTest));
    add(new JButton(dataflowStartStopResumeTest));
  }

  @Override
  public void onDeactivate() throws Exception {
    removeAll();
  }
}
