package com.neverwinterdp.swing.scribengin.dataflow;

import javax.swing.JPanel;

//TODO: Implement this UI using the SpringLayoutGridJPanel, @see UIActivityView
@SuppressWarnings("serial")
public class UIDataflowDescriptorView extends JPanel {
  private String dataflowId ;
  
  public UIDataflowDescriptorView(String dataflowId) {
    this.dataflowId = dataflowId;
  }
  
}
