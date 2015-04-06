package com.neverwinterdp.scribengin.dataflow.test;

import java.io.PrintStream;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.test.DataflowTestReport.DataflowSinkValidatorReport;
import com.neverwinterdp.scribengin.dataflow.test.hdfs.HDFSReport;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;

public class DataflowHDFSSinkValidator extends DataflowSinkValidator {
  //HDFS reader
  private HDFSReader reader;
 
  @Override
  public StorageDescriptor getSinkDescriptor() {
    StorageDescriptor sink = new StorageDescriptor("HDFS");
    sink.attribute("name", DataflowHDFSSinkValidator.class.getSimpleName());
    //TODO Attributes for HDFS?
    sink.attribute("topic", sinkName);
    return sink;
  }

  @Override
  public void init(ScribenginClient scribenginClient) {
    }

  @Override
  public void run() {
    //TODO HDFS messagecheck tool? 
    reader = createHDFSReader();
    reader.read();
  }

  @Override
  public void runInBackground() {
    //Not possible, will run in main thread instead?
    reader = createHDFSReader();
    reader.runAsDeamon();
  }

  @Override
  public boolean waitForTermination() throws InterruptedException {
    //TODO wait until write is done
    return reader.waitForTermination();
  }

  @Override
  public boolean waitForTermination(long timeout) throws InterruptedException {
    //TODO not possible for batch
    return reader.waitForTermination(timeout);
  }

  @Override
  public void populate(DataflowTestReport report) {
    DataflowSinkValidatorReport sinkReport = report.getSinkValidatorReport();
    sinkReport.setSinkName(reader.getReport().getName());
    sinkReport.setNumberOfStreams(reader.getReport().getNumOfStreams());
/*    sinkReport.setReadCount(reader.getReport().getReaderReport().getReadCount());
    sinkReport.setDuration(reader.getReport().getReaderReport().getRunDuration());*/
    try {
      reader.dump(System.out);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  HDFSReader createHDFSReader() {
    String path = "/hdfs/path";
    HDFSReader HDFSReader = new HDFSReader(path, 0, path);
    return HDFSReader;
  }

  class HDFSReader {

    private String path;
    private String connect;

    public HDFSReader(String host, int port, String path) {
      this.connect = "hdfs://" + host + ":" + port;
      this.path = path;
    }

    public void read() {
      //TODO read the folder, read the files
    }

    public void dump(PrintStream out) {
      // TODO dump report

    }

    public boolean waitForTermination(long timeout) {
      // TODO Auto-generated method stub
      return false;
    }

    public boolean waitForTermination() {
      // TODO Auto-generated method stub
      return false;
    }

    public void runAsDeamon() {
      // TODO Auto-generated method stub

    }

    public HDFSReport getReport() {

      return new HDFSReport();
    }

  }
}
