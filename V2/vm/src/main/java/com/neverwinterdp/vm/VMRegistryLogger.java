package com.neverwinterdp.vm;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.RegistryLogger;

public class VMRegistryLogger extends RegistryLogger {

  public VMRegistryLogger(Registry registry, VMDescriptor vmDescriptor, String name) throws RegistryException {
    super(registry, "/logger/" + vmDescriptor.getId() + "/" + name);
  }

}
