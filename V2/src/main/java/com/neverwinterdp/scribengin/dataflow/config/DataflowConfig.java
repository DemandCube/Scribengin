package com.neverwinterdp.scribengin.dataflow.config;


public class DataflowConfig {
  private String         name;
  private SourceConfig[] sourceConfig;
  private SinkConfig[]   sinkConfig;
  private int            numberOfWorkers;
  private int            numberOfTasks;
  private String         dataflowExecutor ;

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public SourceConfig[] getSourceConfig() { return sourceConfig; }
  public void setSourceConfig(SourceConfig[] sourceConfig) { this.sourceConfig = sourceConfig; }
  
  public SinkConfig[] getSinkConfig() { return sinkConfig; }
  public void setSinkConfig(SinkConfig[] sinkConfig) { this.sinkConfig = sinkConfig; }
  
  public int getNumberOfWorkers() { return numberOfWorkers; }
  public void setNumberOfWorkers(int numberOfWorkers) {
    this.numberOfWorkers = numberOfWorkers;
  }
  
  public int getNumberOfTasks() { return numberOfTasks; }
  public void setNumberOfTasks(int numberOfTasks) {
    this.numberOfTasks = numberOfTasks;
  }
  
  public String getDataflowExecutor() { return dataflowExecutor; }
  public void setDataflowExecutor(String dataflowExecutor) {
    this.dataflowExecutor = dataflowExecutor;
  }
}
