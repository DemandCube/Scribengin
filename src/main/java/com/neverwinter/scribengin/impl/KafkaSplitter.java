package com.neverwinter.scribengin.impl;

import java.util.Collection;

import kafka.javaapi.message.ByteBufferMessageSet;

import org.apache.commons.chain.Context;

import com.neverwinter.scribengin.api.Splitter;

// For now we dont use any Splitters.
// Or should one instance only handle one type of source to end?
public class KafkaSplitter implements Splitter<ByteBufferMessageSet> {

  @Override
  public boolean execute(Context context) throws Exception {
    split((ByteBufferMessageSet) context.get("Stream"));
    return false;
  }

  @Override
  public Collection<ByteBufferMessageSet> split(ByteBufferMessageSet t) {
    // how many do i want?
    // how do i get them?
    // what do i do with them?
    return null;
  }

}
