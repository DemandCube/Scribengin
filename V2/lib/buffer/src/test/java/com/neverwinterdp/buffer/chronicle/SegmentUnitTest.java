package com.neverwinterdp.buffer.chronicle;

import org.junit.Test;

import com.neverwinterdp.buffer.chronicle.JavaSerializer;
import com.neverwinterdp.buffer.chronicle.Segment;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.yara.MetricPrinter;
import com.neverwinterdp.yara.MetricRegistry;
import com.neverwinterdp.yara.Timer;

public class SegmentUnitTest {
  @Test
  public void testSegment() throws Exception {
    String segmentDir = "build/segment" ;
    FileUtil.removeIfExist(segmentDir, false);
    Segment<String> segment = new Segment<String>(segmentDir, new JavaSerializer<String>(), 0, 10000) ;
    segment.open();
    for(int i = 0; i < 10; i++) {
      segment.append("This is a test , This is a test, This is a test" + i);
    }
    segment.close();
    
    segment.open();
    int read = 0 ;
    while(segment.hasNext() && read < 5) {
      String text = segment.nextObject() ;
      System.out.println("text: " + text);
      read++ ;
    }
    segment.close();
  }
  
  @Test
  public void testPerformanceSegment() throws Exception {
    String segmentDir = "build/segment" ;
    FileUtil.removeIfExist(segmentDir, false);
    MetricRegistry mRegistry = new MetricRegistry() ;
    Segment<String> segment = new Segment<String>(segmentDir, new JavaSerializer<String>(), 0, 10000) ;
    segment.open();
    int RUN = 10000000 ;
    byte[] data  = new byte[64];
    for(int i = 0; i < RUN; i++) {
      Timer.Context timeCtx = mRegistry.timer("queue", "write").time() ;
      segment.append(data);
      timeCtx.close(); 
    }
    segment.close();
    
    segment.open();
    while(segment.hasNext()) {
      Timer.Context timeCtx = mRegistry.timer("queue", "next").time() ;
      byte[] bytes = segment.next();
      timeCtx.close(); 
    }
    segment.close();
    new MetricPrinter().print(mRegistry);
  }
}
