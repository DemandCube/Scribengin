package com.neverwinterdp.swing;

public interface UILifecycle {
  public void onInit() throws Exception ;
  public void onDestroy() throws Exception ;
  public void onActivate() throws Exception ;
  public void onDeactivate() throws Exception ;
}
