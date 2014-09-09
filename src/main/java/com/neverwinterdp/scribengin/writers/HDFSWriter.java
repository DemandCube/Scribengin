package com.neverwinterdp.scribengin.writers;

import java.io.IOException;

import org.apache.commons.chain.Chain;
import org.apache.commons.chain.Context;
import org.apache.log4j.Logger;

import com.neverwinterdp.scribengin.ScribenginContext;
import com.neverwinterdp.scribengin.writer.helpers.StringRecordWriter;

// TODO factor in buffering
// TODO make it quick
public class HDFSWriter implements Writer<byte[]> {

  private static final Logger logger = Logger.getLogger(HDFSWriter.class);
  private ScribenginContext scribenginContext;
  private StringRecordWriter writer;

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
  
  @Override
  public void write(byte[] data) throws IOException {
    logger.info("WRITE");
    writer.write(data);
    writer.close();
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
