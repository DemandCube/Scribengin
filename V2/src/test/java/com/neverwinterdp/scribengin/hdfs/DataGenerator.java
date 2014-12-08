package com.neverwinterdp.scribengin.hdfs;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;

public class DataGenerator {
  static public void generateNewStream(Sink sink, int numOfBuffer, int numOfRecordPerBuffer) throws Exception {
    SinkStream stream = sink.newStream();
    SinkStreamWriter writer = stream.getWriter();
    for(int i = 0; i < numOfBuffer; i++) {
      for(int j = 0; j < numOfRecordPerBuffer; j ++) {
        String key = "stream=" + stream.getDescriptor().getId() +",buffer=" + i + ",record=" + j;
        writer.append(createRecord(key, key));
      }
      writer.commit();
    }
    writer.close();
  }
  
  static public Record createRecord(String key, String text) {
    byte[] data = text.getBytes();
    Record record = new Record(key, data) ;
    return record;
  }
}
