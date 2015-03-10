package com.neverwinterdp.scribengin.sink;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.scribengin.s3.sink.S3Sink;
import com.neverwinterdp.scribengin.util.PropertyUtils;

public class S3SinkUnitTest {

  @Test
  public void test() throws Exception {
    SinkStreamDescriptor descriptor = new PropertyUtils("s3.default.properties").getDescriptor();
    descriptor.setLocation("test");
    Injector injector  = Guice.createInjector(new S3TestModule(descriptor));
    S3Sink S3SinkStream = new S3Sink(injector, descriptor);
    SinkStream stream = S3SinkStream.newStream();
    SinkStreamWriter writer = stream.getWriter();
    assertNotNull(writer);
  
  }
  
}
