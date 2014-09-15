package com.neverwinterdp.scribengin;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;


/**
 * Helper object to write to HDFS
 * Only will ever write to one file, as defined by the constructor
 * @author Richard Duarte
 *
 */
public class StringRecordWriter {
  private FSDataOutputStream os;
  private FileSystem fs;
  private Configuration conf;
  private String hdfsPath;
  private boolean opened=false;
  /**
   * Constructor
   * @param hdfsPath Path of HDFS file to write to
   * @param resources Resource files to add to Configuration()
   */
  public StringRecordWriter(String hdfsPath, String[] resources) {
    conf = new Configuration();
    for(int i=0; i<resources.length; i++){
      Preconditions.checkArgument(!new File(resources[i]).exists(), "Path to "+resources[i]+" does not exist");
      conf.addResource(resources[i]);
    }
    this.hdfsPath = hdfsPath;
    
  }
  
  /**
   * Default constructor, sets default hadoop xml config files
   * @param hdfsPath Path of file to write to
   */
  public StringRecordWriter(String hdfsPath){
    this(hdfsPath, new String[0]);
  }

  /**
   * Writes, flushes, and closes file handle to HDFS
   * @param bytes byte array to write to HDFS
   * @throws IOException
   */
  public void write(byte[] bytes) throws IOException {
    opened = true;
    fs = FileSystem.get(URI.create(this.hdfsPath), conf);
    Path path = new Path(this.hdfsPath);
    
    if (fs.exists(path)) {
      os = fs.append(path);
    } else {
      os = fs.create(path);
    }
    os.write(bytes);
    os.flush();
  }

  /**
   * Closes connection
   */
  public void close() {
    if(!opened){
      return;
    }
    
    try {
      os.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      fs.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    opened = false;
  }
}