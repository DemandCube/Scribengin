package com.neverwinterdp.scribengin.dataflow.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.test.DataflowTestReport.DataflowSourceGeneratorReport;
import com.neverwinterdp.scribengin.dataflow.test.hdfs.HDFSReport;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;

public abstract class DataflowHDFSSourceGeneratorAnthony extends DataflowSourceGenerator {

  private HDFSWriter writer;

  @Override
  public void init(ScribenginClient scribenginClient) {
    String directory = null;
    int port = 0;
    String host = null;
    //TODO init
    writer = new HDFSWriter(host, port, directory);
    setNumberOfGeneratedRecords(numberOfStream * maxRecordsPerStream);
  }

  @Override
  public void run() {
    try {
      writer.write();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void runInBackground() {
    try {
      writer.write();
    } catch (IOException e) {
    }
  }

  @Override
  public void populate(DataflowTestReport report) {
    HDFSReport topicReport = writer.getReport();
    DataflowSourceGeneratorReport sourceReport = report.getSourceGeneratorReport();
    sourceReport.setSourceName(topicReport.getName());
    sourceReport.setNumberOfStreams(topicReport.getNumOfStreams());
    /*    sourceReport.setWriteCount(topicReport.getWriterReport().getMessageSent());
        sourceReport.setDuration(topicReport.getWriterReport().getRunDuration());*/
  }

  @Override
  public StorageDescriptor getSourceDescriptor() {
    //TODO same as validator?
    StorageDescriptor sourceDescriptor = new StorageDescriptor("kafka");
    sourceDescriptor.attribute("name", "DataflowKafkaSourceGenerator");
    sourceDescriptor.attribute("topic", sourceName);
    return sourceDescriptor;
  }

  class HDFSWriter {

    private HDFSMessageGenerator generator;
    private int messageSize;
    private Path path;

    public HDFSWriter(String host, int port, String directory) {
      path = new Path("hdfs://" + host + ":" + port + "/" + directory);
      generator = new HDFSMessageGenerator(messageSize);
    }

    //TODO write files
    public void write() throws IOException {
      boolean continueWriting = true;
      BufferedWriter br = null;
      while (continueWriting) {
        //TODO this is where we write
        //write messageGenerator.next
      }
      br.close();
    }

    //TODO return HDFSReport
    public HDFSReport getReport() {

      return null;
    }

    class HDFSMessageGenerator {

      private int messageTracker = 0;
      private int messageSize;

      public HDFSMessageGenerator(int messageSize) {
        this.messageSize = messageSize;
      }

      //TODO return a file of given name and of message size
      //TODO create file of size once.
      public File next() throws IOException {
        File file = new File(String.valueOf(messageTracker++));
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        fileOutputStream.write(new byte[messageSize]);
        fileOutputStream.close();
        return file;
      }
    }
  }
}
