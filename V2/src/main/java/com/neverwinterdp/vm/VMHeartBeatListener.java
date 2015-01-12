package com.neverwinterdp.vm;


public interface VMHeartBeatListener {
  public void onConnected(VMDescriptor vmDescriptor) ;
  public void onDisconnected(VMDescriptor vmDescriptor);
}
