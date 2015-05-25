package com.neverwinterdp.buffer.chronicle;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.LinkedList;

import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.yara.MetricRegistry;
import com.neverwinterdp.yara.Timer;

public class MultiSegmentQueue<T> {
  private String                 storeDir;
  private LinkedList<Segment<T>> segments;
  private Segment<T>             writting;
  private Serializer<T>          serializer;
  private long                   maxSizePerSegment;
  private int                    segmentIndexTracker = 0;

  public MultiSegmentQueue(String storeDir, long maxSizePerSegment) throws Exception {
    this(storeDir, new JavaSerializer<T>(), maxSizePerSegment) ;
  }
  
  public MultiSegmentQueue(String storeDir, Serializer<T> serializer, long maxSizePerSegment) throws Exception {
    this.storeDir = storeDir ;
    this.serializer = serializer ;
    this.maxSizePerSegment = maxSizePerSegment ;
    segments = new LinkedList<Segment<T>>() ;
    if(!FileUtil.exist(storeDir)) {
      FileUtil.mkdirs(storeDir) ;
    } else {
      File dir = new File(storeDir) ;
      File[] fsegments = dir.listFiles(new SegmentFileFilter()) ;
      for(File selSegment : fsegments) {
        String fileName = selSegment.getName() ;
        String numString = fileName.substring("segment-".length(), fileName.lastIndexOf('.')) ;
        int segIndex = Integer.parseInt(numString) ;
        Segment<T> segment = new Segment<T>(storeDir, serializer, segIndex, maxSizePerSegment) ;
        segments.add(segment) ;
        if(segmentIndexTracker < segIndex) segmentIndexTracker = segIndex + 1 ;
      }
    }
  }
  
  synchronized public void writeObject(T object) throws Exception {
    writtingSegment().append(object) ;
  }
  
  synchronized public void write(byte[] data) throws Exception {
    writtingSegment().append(data) ;
  }
  
  synchronized public void write(byte[] data, MetricRegistry mRegistry) throws Exception {
    Timer.Context timeCtx = mRegistry.timer("queue", "segment", "find").time() ;
    Segment<T> segment = writtingSegment();
    timeCtx.stop() ;
    
    timeCtx = mRegistry.timer("queue", "segment", "write").time() ;
    segment.append(data) ;
    timeCtx.stop() ;
  }
  
  Segment<T> writtingSegment() throws Exception {
    if(writting != null && writting.isFull()) {
      closeWritingSegment() ;
    }
    if(writting == null) {
      writting = new Segment<T>(storeDir, serializer, segmentIndexTracker++, maxSizePerSegment) ;
      writting.open() ;
    }
    return writting ;
  }
  
  synchronized public Segment<T> nextReadSegment(long wait) throws InterruptedException, IOException {
    if(segments.size() > 0) return segments.getFirst() ;
    wait(wait) ;
    closeWritingSegment() ;
    if(segments.size() > 0) return segments.getFirst() ;
    return null ;
  }
  
  synchronized public void commitReadSegment(Segment<T> segment) throws Exception {
    segments.remove(segment) ;
    segment.delete();
  }

  void closeWritingSegment() throws IOException {
    if(writting == null) return ;
    writting.close() ;
    segments.addLast(writting);
    notifyAll();
    writting = null ;
  }
  
  synchronized public void close() throws IOException {
    if(writting != null) {
      writting.close() ;
      writting = null ;
    }
  }
  
  static public class SegmentFileFilter implements FileFilter {
    public boolean accept(File pathname) {
      String fname = pathname.getName() ;
      if(fname.startsWith("segment") && fname.endsWith(".index")) return true ;
      return false;
    }
  }
}
