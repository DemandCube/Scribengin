package com.neverwinterdp.tool.message;

import com.neverwinterdp.util.JSONSerializer;


public interface MessageExtractor {
  static public MessageExtractor DEFAULT_MESSAGE_EXTRACTOR = new MessageExtractor() {
    @Override
    public Message extract(byte[] messagePayload) {
      return JSONSerializer.INSTANCE.fromBytes(messagePayload, Message.class) ;
    }
  };
  public Message extract(byte[] message) ;
}
