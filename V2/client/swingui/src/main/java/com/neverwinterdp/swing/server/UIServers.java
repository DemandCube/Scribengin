package com.neverwinterdp.swing.server;

import javax.swing.border.EmptyBorder;

import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.widget.JAccordionPanel;

@SuppressWarnings("serial")
public class UIServers extends JAccordionPanel implements UILifecycle {
  
  public UIServers() throws Exception {
    setOpaque(false);
    setBorder(new EmptyBorder(0, 0, 5, 0) );

    addBar("Hadoop", getDummyPanel("Hadoop"));
    addBar("Zookeeper", getDummyPanel("Zookeeper"));
    addBar("Kafka", getDummyPanel("Kafka"));
  }

  @Override
  public void onInit() throws Exception {
  }

  @Override
  public void onDestroy() throws Exception {
  }

  @Override
  public void onActivate() throws Exception {
    setPreferredSize(getParent().getSize()) ;
    doLayout();
  }

  @Override
  public void onDeactivate() throws Exception {
  }
}
