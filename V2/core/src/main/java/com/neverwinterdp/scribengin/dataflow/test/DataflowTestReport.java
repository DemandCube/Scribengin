package com.neverwinterdp.scribengin.dataflow.test;

import java.io.IOException;

import com.neverwinterdp.util.text.TabularFormater;

public class DataflowTestReport {
  private DataflowSourceGeneratorReport sourceGeneratorReport = new DataflowSourceGeneratorReport();
  private DataflowSinkValidatorReport   sinkValidatorReport = new DataflowSinkValidatorReport();
  
  public DataflowSourceGeneratorReport getSourceGeneratorReport() { return sourceGeneratorReport; }

  public DataflowSinkValidatorReport getSinkValidatorReport() { return sinkValidatorReport; }

  public void report(Appendable out) throws IOException {
    String[] header = { "Name", "Value" };

    TabularFormater reportFormater = new TabularFormater(header);
    reportFormater.setTitle("Dataflow Test Report ");
    reportFormater.addRow("Source", sourceGeneratorReport.getSourceName());
    reportFormater.addRow("    Number Of Streams", sourceGeneratorReport.getNumberOfStreams());
    reportFormater.addRow("    Write Count", sourceGeneratorReport.getWriteCount());
    reportFormater.addRow("    Duration", sourceGeneratorReport.getDuration() + "ms");
    
    reportFormater.addRow("Sink",   sinkValidatorReport.getSinkName());
    reportFormater.addRow("    Number Of Streams",   sinkValidatorReport.getNumberOfStreams());
    reportFormater.addRow("    Read",   sinkValidatorReport.getReadCount());
    reportFormater.addRow("    Duration",   sinkValidatorReport.getDuration() + "ms");
    out.append("\n");
    out.append(reportFormater.getFormatText());
  }
  
  static public class DataflowSourceGeneratorReport {
    private String sourceName;
    private int    numberOfStreams;
    private int    writeCount ;
    private long   duration;
    
    public String getSourceName() { return sourceName; }
    public void setSourceName(String sourceName) { this.sourceName = sourceName; }
    
    public int getNumberOfStreams() { return numberOfStreams;}
    public void setNumberOfStreams(int numberOfStreams) { this.numberOfStreams = numberOfStreams;}
    
    public int getWriteCount() { return writeCount; }
    public void setWriteCount(int writeCount) { this.writeCount = writeCount; }
    
    public long getDuration() { return duration; }
    public void setDuration(long duration) { this.duration = duration; }
  }
  
  static public class DataflowSinkValidatorReport {
    private String sinkName;
    private int    numberOfStreams;  
    private int    readCount ;
    private long   duration ;
    
    public String getSinkName() { return sinkName; }
    public void setSinkName(String sinkName) { this.sinkName = sinkName; }
    
    public int getNumberOfStreams() { return numberOfStreams;}
    public void setNumberOfStreams(int numberOfStreams) { this.numberOfStreams = numberOfStreams;}
    
    public int getReadCount() { return readCount; }
    public void setReadCount(int readCount) { this.readCount = readCount; }
    
    public long getDuration() { return duration; }
    public void setDuration(long duration) { this.duration = duration; }
  }
}
