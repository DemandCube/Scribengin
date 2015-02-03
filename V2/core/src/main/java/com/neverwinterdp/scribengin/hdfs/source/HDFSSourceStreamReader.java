package com.neverwinterdp.scribengin.hdfs.source;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.CommitPoint;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;
import com.neverwinterdp.scribengin.source.SourceStreamReader;
import com.neverwinterdp.util.JSONSerializer;

/**
 * @author Tuan Nguyen
 */
public class HDFSSourceStreamReader implements SourceStreamReader {
  private String name ;
  private FileSystem fs;
  private SourceStreamDescriptor descriptor;
  private List<Path> dataPaths = new ArrayList<Path>();
  private int currentDataPathPos = -1 ;
  private FSDataInputStream currentDataPathInputStream;
  
  private int commitPoint ;
  private int currPosition ;
  
  public HDFSSourceStreamReader(String name, FileSystem fs, SourceStreamDescriptor descriptor) throws FileNotFoundException, IllegalArgumentException, IOException {
    this.name = name ;
    this.fs = fs;
    this.descriptor = descriptor;
    FileStatus[] status = fs.listStatus(new Path(descriptor.getLocation())) ;
    for(int i = 0; i < status.length; i++) {
      dataPaths.add(status[i].getPath());
    }
    currentDataPathInputStream = nextDataPathInputStream();
  }
  
  public String getName() { return name ; }
  
  public Record next() throws Exception {
    if(currentDataPathInputStream.available() <= 0) {
      currentDataPathInputStream.close();
      currentDataPathInputStream = nextDataPathInputStream();
    }
    if(currentDataPathInputStream == null) return null ;
    int recordSize = currentDataPathInputStream.readInt();
    byte[] data = new byte[recordSize] ;
    currentDataPathInputStream.readFully(data);
    return JSONSerializer.INSTANCE.fromBytes(data, Record.class);
  }
  
  public Record[] next(int size) throws Exception {
    List<Record> holder = new ArrayList<Record>() ;
    Record[] array = new Record[holder.size()] ;
    for(int i = 0; i < size; i++) {
      Record record = next();
      if(record != null) holder.add(record) ;
      else break;
    }
    holder.toArray(array) ;
    return array ;
  }

  public void rollback() throws Exception {
    System.err.println("This method is not implemented");
    currPosition = commitPoint ;
  }
  
  public CommitPoint commit() throws Exception {
    System.err.println("This method is not implemented");
    CommitPoint cp = new CommitPoint(name, commitPoint, currPosition) ;
    this.commitPoint = currPosition ;
    return cp ;
  }
  
  public void close() throws Exception {
  }
  
  private FSDataInputStream nextDataPathInputStream() throws IOException {
    currentDataPathPos++;
    if(currentDataPathPos >= dataPaths.size()) return null;
    FSDataInputStream is = fs.open(dataPaths.get(currentDataPathPos));
    return is;
  }
}