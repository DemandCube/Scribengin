package com.neverwinterdp.scribengin.stream.sink;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import scala.Array;

import com.neverwinterdp.scribengin.tuple.Tuple;

public class SimpleFileBuffer implements DiskBuffer {
  List<File> files = new ArrayList<>();
  private File singleFile;
  Writer writer;




  public SimpleFileBuffer() {
    try {
      singleFile = File.createTempFile("tmpfile"+ UUID.randomUUID().toString(), ".log");
      writer = new OutputStreamWriter(new FileOutputStream(singleFile));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  @Override
  public List<File> getAllFiles() {
    return files;
  }


  @Override
  public void add(Tuple tuple) {

    try {
        writer.write(tuple.getData().toString());
        writer.flush();

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }


  @Override
  public int getTuplesCount() {
    // TODO Auto-generated method stub
    return 0;
  }


  @Override
  public int getTuplesSize() {
    // TODO Auto-generated method stub
    return 0;
  }


  @Override
  public long getDuration() {
    // TODO Auto-generated method stub
    return 0;
  }


  @Override
  public void clean() {
    // TODO Auto-generated method stub
    
  }




}
