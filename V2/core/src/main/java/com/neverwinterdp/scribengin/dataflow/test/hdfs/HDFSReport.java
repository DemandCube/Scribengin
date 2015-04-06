package com.neverwinterdp.scribengin.dataflow.test.hdfs;

//TODO: what is this for????
public class HDFSReport {

  private WriterReport writerReport;
  private ReaderReport readerReport;

  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  public WriterReport getWriterReport() {
    return writerReport;
  }

  public ReaderReport getReaderReport() {
    return readerReport;
  }

  public int getNumOfStreams() {
    // TODO Auto-generated method stub
    return 0;
  }

   class WriterReport {

    public int getMessageSent() {
      // TODO Auto-generated method stub
      return 0;
    }

    public long getRunDuration() {
      // TODO Auto-generated method stub
      return 0;
    }
  }

   class ReaderReport {

    public int getReadCount() {
      // TODO Auto-generated method stub
      return 0;
    }

    public long getRunDuration() {
      // TODO Auto-generated method stub
      return 0;
    }

  }
}