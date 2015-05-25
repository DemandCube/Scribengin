package com.neverwinterdp.vm;

import java.util.ArrayList;
import java.util.List;

abstract public class VMApp {
  static public enum TerminateEvent { Shutdown, SimulateKill, Kill }
  
  private VM vm;
  private boolean waitForTerminate = false ;
  private List<VMAppTerminateEventListener> listeners = new ArrayList<>();
  
  public VM getVM() { return this.vm ; }
  public void setVM(VM vm) { this.vm = vm ; }
  
  abstract public void run() throws Exception ;
  
  public boolean isWaittingForTerminate() { return waitForTerminate ; }
  
  synchronized public void waitForTerminate() throws InterruptedException {
    waitForTerminate = true;
    wait(0);
  }
  
  synchronized public void terminate(TerminateEvent terminateEvent) {
    for(VMAppTerminateEventListener listener : listeners) {
      listener.onEvent(this, terminateEvent);
    }
    notify();
  }
  
  public void addListener(VMAppTerminateEventListener listener) {
    listeners.add(listener);
  }
  
  static public interface VMAppTerminateEventListener {
    public void onEvent(VMApp vmApp, TerminateEvent terminateEvent) ;
  }
}