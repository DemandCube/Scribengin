package com.neverwinterdp.scribengin.writer;

import java.io.DataOutputStream;
import java.io.IOException;


/**
 * Record Writer abstract writer.
 * 
 * @author Anthony
 * */
public abstract class RecordWriter {

  protected DataOutputStream dos;

  abstract public void write(byte[] data) throws IOException;

  abstract public void close() throws IOException;
  
}
