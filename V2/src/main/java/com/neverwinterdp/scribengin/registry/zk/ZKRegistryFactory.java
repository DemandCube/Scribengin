package com.neverwinterdp.scribengin.registry.zk;

import com.neverwinterdp.scribengin.registry.Registry;
import com.neverwinterdp.scribengin.registry.RegistryConfig;
import com.neverwinterdp.scribengin.registry.RegistryFactory;

public class ZKRegistryFactory implements RegistryFactory {

  @Override
  public Registry create(RegistryConfig config) {
    return new RegistryImpl(config);
  }

}
