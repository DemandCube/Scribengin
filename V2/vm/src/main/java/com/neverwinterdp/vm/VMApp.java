package com.neverwinterdp.vm;

import java.util.ArrayList;
import java.util.List;

abstract public class VMApp {
  static public enum Event { Shutdown }
  
  private VM vm;
  private boolean waitForShutdown = false ;
  private List<EventListener> listeners = new ArrayList<>();
  
  public VM getVM() { return this.vm ; }
  public void setVM(VM vm) { this.vm = vm ; }
  
  abstract public void run() throws Exception ;
  
  public boolean isWaittingForShutdown() { return waitForShutdown ; }
  
  synchronized public void waitForShutdown() throws InterruptedException {
    waitForShutdown = true;
    wait(0);
  }
  
  synchronized public void notifyShutdown() {
    for(EventListener listener : listeners) {
      listener.onEvent(this, Event.Shutdown);
    }
    notify();
  }
  
  public void addListener(EventListener listener) {
    listeners.add(listener);
  }
  
  static public interface EventListener {
    public void onEvent(VMApp vmApp, Event event) ;
  }
}