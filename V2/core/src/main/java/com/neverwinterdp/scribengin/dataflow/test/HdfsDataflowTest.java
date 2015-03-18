package com.neverwinterdp.scribengin.dataflow.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.test.HelloKafkaDataflowBuilder.TestCopyScribe;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.hdfs.HDFSSourceGenerator;
import com.neverwinterdp.scribengin.kafka.KafkaSourceGenerator;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.source.SourceDescriptor;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;


public class HdfsDataflowTest extends DataflowTest {

  protected void doRun(ScribenginShell shell) throws Exception {
    ScribenginShell scribenginShell = (ScribenginShell) shell;
    FileSystem fs = FileSystem.getLocal(new Configuration());
    HelloHDFSDataflowBuilder hdfDataflowBuilder = new HelloHDFSDataflowBuilder(scribenginShell.getScribenginClient(),
        getDataDir());
    new HDFSSourceGenerator().generateSource(fs, getDataDir() + "/source");
    ScribenginWaitingEventListener eventListener = hdfDataflowBuilder.submit();
    eventListener.waitForEvents(60000);
  }

  private String getDataDir() {
    return "./build/hdfs";
  }
}