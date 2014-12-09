package com.neverwinterdp.scribengin.sink;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class SaveChangesToMemoryMappedByteBufferFromFileChannel {
    
    public static void main(String[] args) {
        
        try {
            
            File file = new File("myfile.dat");
            
            // create a random access file stream (read-write)
            FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
            
            // map a region of this channel's file directly into memory
            MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_WRITE, 0, (int) channel.size());

            // write the given byte into this buffer at the current
            // position, and then increment the position
            buf.put((byte)0x01);

            // force any changes made to this buffer's content to be written
            // to the storage device containing the mapped file
            buf.force();

            // close the channel
            channel.close();
            
        }
        catch (IOException e) {
            System.out.println("I/O Error: " + e.getMessage());
        }
        
    }

}