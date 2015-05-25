package com.neverwinterdp.es;

import java.util.HashMap;
import java.util.Map;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import com.mycila.guice.ext.closeable.CloseableModule;
import com.mycila.guice.ext.jsr250.Jsr250Module;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.module.MycilaJmxModuleExt;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.util.LoggerFactory;

public class ModuleMain {
  static public class Configuration {
    @ParametersDelegate
    private RegistryConfig      registryConfig = RegistryConfig.getDefault();

    @DynamicParameter(names = "--es", description = "Elasticsearch properties configuration")
    private Map<String, String> esProperties   = new HashMap<>() ;
  }
  
  static public void main(String[] args) throws Exception {
    if(args == null || args.length == 0) {
      args = new String[] {
          "--es:server.name=elasticsearch-1",
          "--es:path.data=./build/elasticsearch-1"
      } ;
    }
    final Configuration conf = new Configuration() ;
    new JCommander(conf, args);
    AppModule module = new AppModule(new HashMap<String, String>()) {
      @Override
      protected void configure(Map<String, String> properties) {
        bindInstance(RegistryConfig.class, conf.registryConfig) ;
        bindMapProperties("esProperties", conf.esProperties) ;
        bindInstance(LoggerFactory.class, new LoggerFactory("elasticsearch")) ;
      };
    };
    Module[] modules = {
      new CloseableModule(),new Jsr250Module(), new MycilaJmxModuleExt("elasticsearch"),  module
    };
    Injector container = Guice.createInjector(Stage.PRODUCTION, modules);
    ElasticSearchService searchService = container.getInstance(ElasticSearchService.class);
    searchService.start();
    Thread.currentThread().join();
  }
}
