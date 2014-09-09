package com.neverwinterdp.scribengin.converters;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.apache.commons.chain.Chain;
import org.apache.commons.chain.Context;
import org.apache.log4j.Logger;

import com.neverwinterdp.scribengin.ScribenginContext;


// What is Avro?
@SuppressWarnings("unchecked")
public class KafKaToHDFSConverter implements
    Converter<ByteBufferMessageSet, byte[]> {

  private static final Logger logger = Logger
      .getLogger(KafKaToHDFSConverter.class);
  private ScribenginContext scribenginContext;

  @Override
  public byte[] convert(ByteBufferMessageSet byteBufferMessageSet) {
    logger.info("Trying to convert");
    byte[] bytes = null;
    for (MessageAndOffset messageAndOffset : byteBufferMessageSet) {
      ByteBuffer payload = messageAndOffset.message().payload();
     
      bytes = new byte[payload.limit()];
      payload.get(bytes);
  //    logger.info("Offset " + messageAndOffset.offset()  +" payload " + new String(bytes, Charset.defaultCharset()));
    }
    return bytes;
  }



  @Override
  public boolean execute(Context context) throws Exception {
    scribenginContext = (ScribenginContext) context;
    byte[] writerData =
        convert((ByteBufferMessageSet) scribenginContext.get(ScribenginContext.CONVERTER_DATA));

    scribenginContext.put(ScribenginContext.WRITER_DATA, writerData);
    context.putAll(scribenginContext);
    return Chain.CONTINUE_PROCESSING;
  }
}
