package com.neverwinterdp.scribengin.dataflow.worker;

public interface EventListener {
  static public enum Event { INIT, DESTROY }

  public void onEvent(DataflowTaskExecutorManager worker, Event event);
}
