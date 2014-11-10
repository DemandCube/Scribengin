package com.neverwinterdp.scribengin.source;


import org.junit.Test;
import org.junit.Assert;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.CommitPoint;
import com.neverwinterdp.scribengin.source.Source;
import com.neverwinterdp.scribengin.source.SourceStreamReader;
import com.neverwinterdp.scribengin.source.ri.SourceImpl;

/**
 * @author Tuan Nguyen
 */
public class DataSourceUnitTest {
  @Test
  public void testDataSource() throws Exception {
    SourceDescriptor config = new SourceDescriptor();
    config.setName("RI");
    Source datasource = new SourceImpl(config, 3, 100) ;
    SourceStream[] streams = datasource.getSourceStreams() ;
    Assert.assertEquals(3, streams.length);
    
    SourceStreamReader reader = streams[0].getReader("test") ;
    for(int i = 0 ; i < 25; i++) {
      Record record = reader.next() ;
      Assert.assertNotNull(record);
    }
    CommitPoint commitPoint1 = reader.commit(); 
    Assert.assertEquals(25, commitPoint1.getEndOffset()) ;
    
    for(int i = 0 ; i < 25; i++) {
      Record record = reader.next() ;
      Assert.assertNotNull(record);
    }
    reader.rollback(); 
    
    int readCount = 0 ;
    Record record = null ;
    while((record = reader.next()) != null) {
      Assert.assertNotNull(record);
      readCount++ ;
    }
    CommitPoint commitPoint2 = reader.commit();
    Assert.assertEquals(commitPoint1.getEndOffset(), commitPoint2.getStartOffset()) ;
    Assert.assertEquals(75, readCount) ;
  }
}
