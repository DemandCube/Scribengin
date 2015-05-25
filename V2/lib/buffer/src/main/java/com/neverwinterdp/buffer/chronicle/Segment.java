package com.neverwinterdp.buffer.chronicle;

import java.io.IOException;

import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.tools.ChronicleTools;

public class Segment<T> {
  //16 billion max, or one per day for 11 years.
  public static final ChronicleConfig SMALL = ChronicleConfig.SMALL ;
  
  private IndexedChronicle chronicle ;
  private ExcerptTailer    reader ;
  private ExcerptAppender  appender ;
  private Serializer<T>    serializer ;
  
  private String storeDir ;
  private int  segmentIndex ;
  private long maxSize = 8 * 1024;
  private long size =  0; 
  
  public Segment(String storeDir, Serializer<T> serializer, int idx, long maxSize) {
    this.storeDir = storeDir ;
    this.serializer = serializer ;
    this.maxSize = maxSize ;
    this.segmentIndex = idx ;
  }

  public int getSegmentIndex() { return segmentIndex; }
  public void setSegmentIndex(int index) { this.segmentIndex = index; }

  public boolean isFull() throws Exception {
    return size > maxSize;
  }
  
  public boolean hasNext() throws Exception {
    return reader.nextIndex() ;
  }
  
  public byte[] next() throws Exception {
    int len = reader.readInt() ;
    byte[] data =  new byte[len] ;
    reader.read(data) ;
    return data ;
  }
  
  public T nextObject() throws Exception {
    byte[] data =  next() ;
    T object = serializer.fromBytes(data) ;
    return object ;
  }
  
  public void append(byte[] data) throws Exception {
    appender.startExcerpt();
    appender.writeInt(data.length);
    appender.write(data);
    appender.finish();
    size++ ;
  }
  
  public void append(T object) throws Exception {
    append(serializer.toBytes(object)) ;
  }
  
  public void delete() throws Exception {
    close() ;
    ChronicleTools.deleteOnExit(storeDir + "/segment-" + segmentIndex);
  }
  
  public void open() throws Exception {
    if(chronicle != null) return ;
    chronicle = new IndexedChronicle(storeDir + "/segment-" + segmentIndex, SMALL) ;
    appender = chronicle.createAppender();
    reader = chronicle.createTailer() ;
  }
  
  public void close() throws IOException {
    if(chronicle != null) {
      appender.close();
      reader.close();
      chronicle.close() ;
      chronicle = null ;
    }
  }
}