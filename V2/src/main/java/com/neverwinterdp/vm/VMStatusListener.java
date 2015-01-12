package com.neverwinterdp.vm;



public interface VMStatusListener {
  public void onChange(VMDescriptor descriptor, VMStatus status) ;
}
