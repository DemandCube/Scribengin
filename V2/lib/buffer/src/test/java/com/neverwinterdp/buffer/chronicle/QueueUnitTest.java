package com.neverwinterdp.buffer.chronicle;

import org.junit.Test;

import com.neverwinterdp.buffer.chronicle.MultiSegmentQueue;
import com.neverwinterdp.buffer.chronicle.Segment;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.yara.MetricPrinter;
import com.neverwinterdp.yara.MetricRegistry;
import com.neverwinterdp.yara.Timer;

public class QueueUnitTest {
  @Test
  public void testQueue() throws Exception {
    MetricRegistry mRegistry = new MetricRegistry() ;
    byte[] data = generateData(1024) ;
    
    System.out.println("Warm up");
    testSegment(data, 10000, mRegistry) ;
    testQueue(data, 1000, 10000, mRegistry) ;
    new MetricPrinter().print(mRegistry);

    System.out.println("Run Perf Test");
    mRegistry = new MetricRegistry() ;
    int NUM_OF_ENTRIES = 1 * 1024 * 1024 ;
    int ENTRY_PER_SEGMENT = (128 * 1024) ;
    testInitSegment(NUM_OF_ENTRIES/ENTRY_PER_SEGMENT, mRegistry) ;
    testSegment(data, NUM_OF_ENTRIES, mRegistry) ;
    testQueue(data, ENTRY_PER_SEGMENT, NUM_OF_ENTRIES, mRegistry) ;
    new MetricPrinter().print(mRegistry);
  }
  
  
  void testQueue(byte[] data, int entryPerSegment, int numOfEntries, MetricRegistry mRegistry) throws Exception {
    FileUtil.removeIfExist("build/queue", false);
    Timer.Context allCtx = mRegistry.timer("all", "queue").time() ;
    MultiSegmentQueue<byte[]> queue = new MultiSegmentQueue<byte[]>("build/queue", entryPerSegment) ;
    Timer.Context testQueueWriteCtx = mRegistry.timer("queue", "writeAll").time() ;
    for(int i = 0; i < numOfEntries; i++) {
      Timer.Context timeCtx = mRegistry.timer("queue", "write").time() ;
      queue.write(data, mRegistry);
      timeCtx.stop(); 
    }
    testQueueWriteCtx.stop();
    
    Segment<byte[]> segment = null ;
    while((segment = queue.nextReadSegment(100)) != null) {
      segment.open() ;
      while(segment.hasNext()) {
        Timer.Context timeCtx = mRegistry.timer("queue", "next").time() ;
        segment.next() ;
        timeCtx.stop(); 
      }
      queue.commitReadSegment(segment);
    }
    queue.close();
    allCtx.stop();
  }
  
  void testSegment(byte[] data, int numOfEntries, MetricRegistry mRegistry) throws Exception {
    String segmentDir = "build/segment" ;
    FileUtil.removeIfExist(segmentDir, false);
    Timer.Context allCtx = mRegistry.timer("all", "segment").time() ;
    Segment<String> segment = new Segment<String>(segmentDir, new JavaSerializer<String>(), 0, numOfEntries) ;
    segment.open();
    Timer.Context writeAllCtx = mRegistry.timer("segment", "writeAll").time() ;
    for(int i = 0; i < numOfEntries; i++) {
      Timer.Context writeCtx = mRegistry.timer("segment", "write").time() ;
      segment.append(data);
      writeCtx.close(); 
    }
    segment.close();
    writeAllCtx.close();
    
    segment.open();
    while(segment.hasNext()) {
      Timer.Context timeCtx = mRegistry.timer("segment", "next").time() ;
      byte[] bytes = segment.next();
      timeCtx.close(); 
    }
    segment.close();
    allCtx.close();
  }
  
  void testInitSegment(int numOfSegment, MetricRegistry mRegistry) throws Exception {
    String segmentDir = "build/segment" ;
    for(int i = 0; i < numOfSegment; i++) {
      FileUtil.removeIfExist(segmentDir, false);
      Timer.Context constructCtx = mRegistry.timer("segment", "init", "construct").time() ;
      Segment<String> segment = new Segment<String>(segmentDir, new JavaSerializer<String>(), 0, 10000) ;
      constructCtx.close();
      
      Timer.Context openCtx = mRegistry.timer("segment", "init", "open").time() ;
      segment.open();
      openCtx.close();
      Timer.Context closeCtx = mRegistry.timer("segment", "init", "close").time() ;
      segment.close();
      closeCtx.close();
    }
  }
  
  
  byte[] generateData(int size) {
    byte[] data = new byte[size] ;
    for(int i = 0; i < data.length; i++) {
      data[i] = (byte)((i % 32) + 32) ;
    }
    return data ;
  }
}