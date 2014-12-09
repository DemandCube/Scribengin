package com.neverwinterdp.scribengin.sink;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class CreateMemoryMappedFile {

  public static void main(String[] args) {

    try {

      File file = new File("myfile.dat");

      // create a random access file stream (for read only)
      FileChannel readOnlyChannel = new RandomAccessFile(file, "r").getChannel();
      // map a region of this channel's file directly into memory
      ByteBuffer readOnlyBuf =

      readOnlyChannel.map(FileChannel.MapMode.READ_ONLY, 0, (int) readOnlyChannel.size());

      // create a random access file stream (read-write)
      FileChannel readWriteChannel = new RandomAccessFile(file, "rw").getChannel();
      // map a region of this channel's file directly into memory
      ByteBuffer readWriteBuf =

      readWriteChannel.map(FileChannel.MapMode.READ_WRITE, 0, (int) readWriteChannel.size());

      // create a random access file stream (private/copy-on-write))
      FileChannel privateChannel = new RandomAccessFile(file, "rw").getChannel();
      // map a region of this channel's file directly into memory
      ByteBuffer privateBuf =

      privateChannel.map(FileChannel.MapMode.PRIVATE, 0, (int) privateChannel.size());

    } catch (IOException e) {
      System.out.println("I/O Error: " + e.getMessage());
    }

  }

}