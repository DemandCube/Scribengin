package com.neverwinter.scribengin.impl;

import java.io.IOException;

import org.apache.commons.chain.Chain;
import org.apache.commons.chain.Context;
import org.apache.log4j.Logger;

import com.neverwinter.scribengin.api.ScribenginContext;
import com.neverwinter.scribengin.api.Writer;

// TODO factor in buffering
// TODO make it quick

@SuppressWarnings("unchecked")
public class HDFSWriter implements Writer<byte[]> {

  private static final Logger logger = Logger.getLogger(HDFSWriter.class);
  private ScribenginContext scribenginContext;
  private StringRecordWriter writer;
  private String uri;


  @Override
  public void write(byte[] data) throws IOException {
    logger.info("write.");
    uri = scribenginContext.getHDFSPath();
    writer = new StringRecordWriter(uri);
    writer.write(data);
    writer.close();
  }

  @Override
  public boolean execute(Context context) throws Exception {
    scribenginContext = (ScribenginContext) context;
    write((byte[]) scribenginContext.get(ScribenginContext.WRITER_DATA));
    context.putAll(scribenginContext);
    return Chain.CONTINUE_PROCESSING;
  }
}
