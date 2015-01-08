package com.neverwinterdp.scribengin.dataflow;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.scribengin.sink.SinkFactory;
import com.neverwinterdp.scribengin.source.SourceFactory;

public class DataflowContainer {
  private Logger logger = LoggerFactory.getLogger(DataflowContainer.class);
  
  @Inject
  private Injector appContainer;

  public DataflowContainer() {}
  
  public DataflowContainer(Map<String, String> props) {
    AppModule module = new AppModule(props) {
      @Override
      protected void configure(Map<String, String> properties) {
        try {
          FileSystem fs = FileSystem.getLocal(new Configuration()) ;
          bindInstance(FileSystem.class, fs);
        } catch (Exception e) {
          logger.error("Error:", e);;
        }
      };
    };
    appContainer = Guice.createInjector(module);
  }
  
  public <T> T getInstance(Class<T> type) { return appContainer.getInstance(type); }
  
  public DataflowRegistry getDataflowRegistry() { return appContainer.getInstance(DataflowRegistry.class); }
  
  public SourceFactory getSourceFactory() { return appContainer.getInstance(SourceFactory.class); }
  
  public SinkFactory getSinkFactory() { return appContainer.getInstance(SinkFactory.class); }
}
