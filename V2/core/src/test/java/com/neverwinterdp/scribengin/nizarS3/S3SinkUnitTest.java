package com.neverwinterdp.scribengin.nizarS3;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.scribengin.nizarS3.sink.S3Sink;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.scribengin.util.PropertyUtils;

/**
 * The Class S3SinkTest.
 */

public class S3SinkUnitTest {
  @Test
  public void test() throws Exception {
    StreamDescriptor descriptor = new PropertyUtils("s3.default.properties").getDescriptor();
    descriptor.setLocation("test");
    Injector injector  = Guice.createInjector(new S3TestModule(descriptor,true));
    S3Sink S3SinkStream = new S3Sink(injector, descriptor);
    SinkStream stream = S3SinkStream.newStream();
    SinkStreamWriter writer = stream.getWriter();
    assertNotNull(writer);
  
  }}
