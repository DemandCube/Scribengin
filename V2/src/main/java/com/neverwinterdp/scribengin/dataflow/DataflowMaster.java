package com.neverwinterdp.scribengin.dataflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.hdfs.sink.SinkImpl;
import com.neverwinterdp.scribengin.hdfs.source.SourceImpl;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.source.Source;
import com.neverwinterdp.scribengin.source.SourceStream;


public class DataflowMaster {
  static String      SOURCE_DIRECTORY       = "./build/hdfs/source";
  static String      SINK_DIRECTORY         = "./build/hdfs/sink";
  static String      INVALID_SINK_DIRECTORY = "./build/hdfs/invalid-sink";
  
  @Inject @Named("dataflow.registry.path")
  private String dataflowRegistryPath;
  
  private DataflowDescriptor dataflowDescriptor;
  
  @Inject
  private Registry registry;
  
  private FileSystem fs ;
  
  @Inject
  public void onInit() throws Exception {
    System.out.println("onInit()");
    System.out.println("  dataflow.registry.path = " + registry);
    System.out.println("  registry               = " + dataflowRegistryPath);
    fs = FileSystem.getLocal(new Configuration()) ;
    dataflowDescriptor = registry.getDataAs(dataflowRegistryPath, DataflowDescriptor.class);
    Node tasksNode   = registry.create(dataflowRegistryPath + "/tasks", NodeCreateMode.PERSISTENT) ;
    Source source    = new SourceImpl(fs, dataflowDescriptor.getSourceDescriptor()) ;
    Map<String, Sink> sinks = new HashMap<String, Sink>();
    for(Map.Entry<String, SinkDescriptor> entry : dataflowDescriptor.getSinkDescriptors().entrySet()) {
      Sink sink = new SinkImpl(fs, entry.getValue());
      sinks.put(entry.getKey(), sink);
    }
    
    SourceStream[] sourceStream = source.getStreams();
    for(int i = 0; i < sourceStream.length; i++) {
      DataflowTaskDescriptor descriptor = new DataflowTaskDescriptor();
      descriptor.setId(i);
      descriptor.setDataProcessor(CopyDataProcessor.class);
      descriptor.setSourceStreamDescriptor(sourceStream[i].getDescriptor());
      for(Map.Entry<String, Sink> entry : sinks.entrySet()) {
        descriptor.add(entry.getKey(), entry.getValue().newStream().getDescriptor());
      }
      tasksNode.createChild("task-", descriptor, NodeCreateMode.PERSISTENT_SEQUENTIAL);
    }
  }
  
  public void onDestroy() throws Exception {
  }
}
