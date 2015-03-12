package com.neverwinterdp.kafka.tool;

import java.io.IOException;

public class KafkaReport {
  private ProducerReport producerReport;
  private ConsumerReport consumerReport ;
  
  public void report(Appendable out) throws IOException {
    out.append("Report");
    //TODO: print out the report in the table format
  }
  
  static public class ProducerReport {
    long duration;
  }
  
  static public class ConsumerReport {
    long duration;
  }
}
