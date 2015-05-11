package com.neverwinterdp.swing.scribengin.dataflow;

import javax.swing.JPanel;

//TODO: Implement this UI using the JXTable.  @see UIActivityListView to see how to create and populate an JXTable

@SuppressWarnings("serial")
public class UIDataflowTaskView extends JPanel {
  private String dataflowId ;
  
  public UIDataflowTaskView(String dataflowId) {
    this.dataflowId = dataflowId;
  }
  
}
