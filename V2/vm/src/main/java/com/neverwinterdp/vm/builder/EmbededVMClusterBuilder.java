package com.neverwinterdp.vm.builder;

import java.util.HashMap;
import java.util.Map;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.server.kafka.KafkaCluster;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.vm.client.LocalVMClient;
import com.neverwinterdp.vm.client.VMClient;

public class EmbededVMClusterBuilder extends VMClusterBuilder {
  private String baseDir = "./build/data";
  protected KafkaCluster kafkaCluster;
  
  public EmbededVMClusterBuilder() throws RegistryException {
    this(new LocalVMClient());
  }
  
  public EmbededVMClusterBuilder(VMClient vmClient) throws RegistryException {
    super(vmClient);
  }
  
  public EmbededVMClusterBuilder(String baseDir, VMClient vmClient) {
    super(vmClient);
    this.baseDir = baseDir ;
  }
  
  @Override
  public void clean() throws Exception {
    super.clean(); 
    FileUtil.removeIfExist(baseDir, false);
  }
  
  @Override
  public void start() throws Exception {
    startKafkaCluster() ;
    super.start();
  }
  
  public void startKafkaCluster() throws Exception {
    h1("Start kafka cluster");
    kafkaCluster = new KafkaCluster(baseDir, 1, 1);
    kafkaCluster.setNumOfPartition(3);
    kafkaCluster.start();
    Thread.sleep(1000);
  }
  
  @Override
  public void shutdown() throws Exception {
    super.shutdown();
    kafkaCluster.shutdown();
  }

  public <T> Injector newAppContainer() {
    Map<String, String> props = new HashMap<String, String>();
    props.put("registry.connect", "127.0.0.1:2181") ;
    props.put("registry.db-domain", "/NeverwinterDP") ;
    
    props.put("implementation:" + Registry.class.getName(), RegistryImpl.class.getName()) ;
    AppModule module = new AppModule(props) ;
    return Guice.createInjector(module);
  }
  
  public RegistryConfig getRegistryConfig() { return vmClient.getRegistry().getRegistryConfig() ; }
  
  public Registry newRegistry() {
    return new RegistryImpl(getRegistryConfig());
  }
}
