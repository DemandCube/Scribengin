package com.neverwinterdp.scribengin.writers;

import java.io.IOException;

import org.apache.commons.chain.Chain;
import org.apache.commons.chain.Context;
import org.apache.log4j.Logger;

import com.neverwinterdp.scribengin.ScribenginContext;
import com.neverwinterdp.scribengin.writer.helpers.StringRecordWriter;

// TODO factor in buffering
// TODO make it quick
/**
 * Writes to HDFS.  Only ever writes to a single file
 * @author Richard Duarte
 * <pre>
 * {@code
 * ScribenginContext context = ....
 * ...
 * 
 * HDFSWriter writer = new HDFSWriter(context);
 * try {
 *   writer.write(testStringBytes);
 * }
 * catch(IOException e) {
 *   System.err.println("Could not write to HDFS");
 * }
 * </pre>
 */
public class HDFSWriter implements Writer<byte[]> {

  private static final Logger logger = Logger.getLogger(HDFSWriter.class);
  private ScribenginContext scribenginContext;
  private StringRecordWriter writer;

  /**
   * Constructor
   * @param con ScribenginContext - <pre>expects context to contain string at getHDFSPath(), 
   * and a list of comma separated values of Hadoop resource files at scribenginContext.getProps().get("hadoop.configFiles") 
   * (i.e. hadoop.configFiles = /etc/hadoop/conf/hdfs-site.xml, /etc/hadoop/conf/core-site.xml )</pre>
   */
  public HDFSWriter(ScribenginContext con){
    super();
    scribenginContext = con;
    if( scribenginContext.getProps().containsKey("hadoop.configFiles") ) {   
      writer = new StringRecordWriter(scribenginContext.getHDFSPath(),scribenginContext.getProps().get("hadoop.configFiles").toString().split(","));
    }
    else{
      writer = new StringRecordWriter(scribenginContext.getHDFSPath());
    }
  }
  
  /**
   * Writes to HDFS
   * @param data to write to 
   */
  @Override
  public void write(byte[] data) throws IOException {
    logger.info("WRITE");
    writer.write(data);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean execute(Context context) throws Exception {
    scribenginContext = (ScribenginContext) context;
    write((byte[]) scribenginContext.get(ScribenginContext.WRITER_DATA));
    context.putAll(scribenginContext);
    return Chain.CONTINUE_PROCESSING;
  }
}
