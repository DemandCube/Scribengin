package com.neverwinterdp.scribengin.source;


import org.junit.Test;
import org.junit.Assert;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.CommitPoint;
import com.neverwinterdp.scribengin.source.DataSource;
import com.neverwinterdp.scribengin.source.DataSourceReader;
import com.neverwinterdp.scribengin.source.ri.DataSourceImpl;

/**
 * @author Tuan Nguyen
 */
public class DataSourceUnitTest {
  @Test
  public void testDataSource() throws Exception {
    DataSource datasource = new DataSourceImpl("RI", 100) ;
    DataSourceReader reader = datasource.getReader() ;
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
