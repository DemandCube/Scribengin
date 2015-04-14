package com.neverwinterdp.scribengin.dataflow.activity;

import java.util.HashMap;
import java.util.Map;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityBuilder;
import com.neverwinterdp.registry.activity.ActivityCoordinator;
import com.neverwinterdp.registry.activity.ActivityService;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.source.Source;
import com.neverwinterdp.scribengin.storage.source.SourceFactory;
import com.neverwinterdp.scribengin.storage.source.SourceStream;

public class DataflowInitActivityBuilder extends ActivityBuilder {
  
  public DataflowInitActivityBuilder( DataflowDescriptor dflDescriptor) {
    getActivity().setDescription("Init Dataflow Activity");
    getActivity().setType("init-dataflow");
    getActivity().withCoordinator(InitActivityCoordinator.class);
    
    add(new ActivityStep().
        withType("init-dataflow-task").
        withExecutor(InitDataflowTaskExecutor.class));
  }
  
  @Singleton
  static public class InitActivityCoordinator extends ActivityCoordinator {
    @Inject
    DataflowActivityStepWorkerService activityStepWorkerService;
   
    @Override
    protected <T> void execute(ActivityService service, Activity activity, ActivityStep step) {
      activityStepWorkerService.exectute(activity, step);
    }
  }
  
  @Singleton
  static public class InitDataflowTaskExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;

    @Override
    public void execute(Activity activity, ActivityStep step) {
      try {
        DataflowDescriptor dataflowDescriptor = service.getDataflowRegistry().getDataflowDescriptor();
        SourceFactory sourceFactory = service.getSourceFactory();
        SinkFactory sinkFactory = service.getSinkFactory() ;

        Source source    = sourceFactory.create(dataflowDescriptor.getSourceDescriptor()) ;
        Map<String, Sink> sinks = new HashMap<String, Sink>();
        for(Map.Entry<String, StorageDescriptor> entry : dataflowDescriptor.getSinkDescriptors().entrySet()) {
          Sink sink = sinkFactory.create(entry.getValue());
          sinks.put(entry.getKey(), sink);
        }

        SourceStream[] sourceStream = source.getStreams();
        for(int i = 0; i < sourceStream.length; i++) {
          DataflowTaskDescriptor descriptor = new DataflowTaskDescriptor();
          descriptor.setId(i);
          descriptor.setScribe(dataflowDescriptor.getScribe());
          descriptor.setSourceStreamDescriptor(sourceStream[i].getDescriptor());
          for(Map.Entry<String, Sink> entry : sinks.entrySet()) {
            descriptor.add(entry.getKey(), entry.getValue().newStream().getDescriptor());
          }
          service.addAvailableTask(descriptor);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
