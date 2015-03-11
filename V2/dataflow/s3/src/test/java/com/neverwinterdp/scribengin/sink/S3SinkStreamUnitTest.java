package com.neverwinterdp.scribengin.sink;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.scribengin.s3.sink.S3SinkStream;
import com.neverwinterdp.scribengin.util.PropertyUtils;

public class S3SinkStreamUnitTest {

  @Test
  public void test() throws Exception {
    
    SinkStreamDescriptor descriptor = new PropertyUtils("s3.default.properties").getDescriptor();
    descriptor.setLocation("");
    Injector injector  = Guice.createInjector(new S3TestModule(descriptor));
    S3SinkStream S3SinkStream = new S3SinkStream(injector, descriptor);
    SinkStreamWriter writer = S3SinkStream.getWriter();
    assertNotNull(writer);
  }

}
