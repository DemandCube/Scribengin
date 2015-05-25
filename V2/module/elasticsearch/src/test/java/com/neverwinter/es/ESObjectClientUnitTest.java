package com.neverwinter.es;

import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceFilterBuilder;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.util.IOUtil;
import com.neverwinterdp.util.text.DateUtil;

/**
 * $Author: Tuan Nguyen$
 **/
public class ESObjectClientUnitTest {
  private Node node ;
  private ESClient esclient ;
  private ESObjectClient<Map<String, Object>> esObjecclient ;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("build/data", false);
    NodeBuilder nb = nodeBuilder();
    nb.getSettings().put("cluster.name", "neverwinterdp");
    nb.getSettings().put("path.data", "build/data");
    node = nb.node();
    esclient = new ESClient(new String[] { "127.0.0.1:9300" });
    esObjecclient = new ESObjectClient<Map<String, Object>>(esclient, "index", Record.class) ;
  }
  
  @After
  public void after() {
    esclient.close();
    node.close();
  }

  @Test
  public void test() throws Exception {
    if (!esObjecclient.isCreated()) {
      esObjecclient.createIndexWith(
          IOUtil.getFileContentAsString("src/test/resources/record.setting.json", "UTF8"), 
          IOUtil.getFileContentAsString("src/test/resources/record.mapping.json", "UTF8")
      );
    }

    esObjecclient.getESClient().getClusterState();
    Date currTime = new Date();
    Date pastTime = DateUtil.parseCompactDate("1/1/2011");
    
    for (int i = 0; i < 10; i++) {
      Date selTime = currTime;
      if (i % 2 == 0) selTime = pastTime;
      Map<String, Object> idoc = createSample("system", selTime, "sample data for elasticsearch");
      esObjecclient.put(idoc, Integer.toString(i));
    }

    Map<String, Map<String, Object>> holder = new LinkedHashMap<String, Map<String, Object>>();
    for (int i = 10; i < 20; i++) {
      Map<String, Object> record = createSample("system", currTime, "sample data for elasticsearch");
      holder.put(Integer.toString(i), record);
    }
    esObjecclient.put(holder);

    Thread.sleep(1000);

    Assert.assertEquals(20, esObjecclient.count(termQuery("createdBy", "system")));
    Assert.assertEquals(20, esObjecclient.count(termQuery("content", "data")));

    SearchResponse sres = esObjecclient.search(termQuery("createdBy", "system"), 0, 10);
    Assert.assertEquals(20, sres.getHits().getTotalHits());
    Assert.assertEquals(10, sres.getHits().hits().length);
    SearchHit[] hit = sres.getHits().hits();
    for (int i = 0; i < 10; i++) {
      System.out.println(new String(hit[i].source()));
      Map<String, Object> record = esObjecclient.getIDocument(hit[i]);
      Map<String, Object> location = (Map<String, Object>)record.get("location");
      Assert.assertEquals(40.73d, location.get("lat"));
      Assert.assertEquals(-74.1d, location.get("lon"));
      System.out.println("Record: " + record.get("content"));
    }

    Assert.assertEquals(20, esObjecclient.count(termQuery("primitive.string", "string")));
    Assert.assertEquals(20, esObjecclient.count(termQuery("primitive.tag", "tag1")));
    Assert.assertEquals(20, esObjecclient.count(termQuery("primitive.tag", "tag2")));
    Assert.assertEquals(20, esObjecclient.count(termQuery("primitive.tag", "colon:colon")));
    Assert.assertEquals(0,  esObjecclient.count(termQuery("primitive.tag", "colon")));

    Assert.assertEquals(20, esObjecclient.count(termQuery("primitive.intValue", 1)));
    Assert.assertEquals(20, esObjecclient.count(termQuery("primitive.intValue", "1")));

    Assert.assertEquals(20, esObjecclient.count(termQuery("primitive.longValue", 1l)));
    Assert.assertEquals(20, esObjecclient.count(termQuery("primitive.longValue", "1")));

    Assert.assertEquals(20, esObjecclient.count(termQuery("primitive.doubleValue", 1d)));
    Assert.assertEquals(20, esObjecclient.count(termQuery("primitive.doubleValue", "1")));

    Assert.assertEquals(20, esObjecclient.count(termQuery("primitive.booleanValue", true)));
    Assert.assertEquals(20, esObjecclient.count(termQuery("primitive.booleanValue", "true")));

    GeoDistanceFilterBuilder geoFilter = FilterBuilders.geoDistanceFilter("location");
    geoFilter.lat(40.73d).lon(-74.1d);
    geoFilter.distance("1km");
    FilteredQueryBuilder geoQuery = filteredQuery(matchAllQuery(), geoFilter);
    Assert.assertEquals(20, esObjecclient.count(geoQuery));

    RangeFilterBuilder numFilter = FilterBuilders.rangeFilter("createdTime");
    numFilter.
      from(pastTime.getTime()).
      includeLower(true).
      to(pastTime.getTime() + 1000 * 60 * 60 * 48).
      cache(false);
    FilteredQueryBuilder numQuery = filteredQuery(matchAllQuery(), numFilter);
    Assert.assertEquals(5, esObjecclient.count(numQuery));

    Assert.assertTrue(esObjecclient.remove("0"));
    Thread.sleep(1000);
    Assert.assertEquals(19, esObjecclient.count(termQuery("createdBy", "system")));

    System.out.println(termQuery("content", "content").toString());
  }

  private Record createSample(String by, Date time, String content) {
    Record record = new Record();
    record.put("createdBy", by);
    record.put("createdTime", time.getTime());
    record.put("content", content);
    
    HashMap<String, Object> primitive = new HashMap<String, Object>() ;
    primitive.put("string",  "string") ;
    primitive.put("text",    "this is a text") ;
    primitive.put("tag",     new String[] {"tag1", "tag2", "colon:colon"}) ;
    primitive.put("intValue",     1) ;
    primitive.put("longValue",    1l) ;
    primitive.put("doubleValue",  1d) ;
    primitive.put("booleanValue", true) ;
    record.put("primitive", primitive) ;
    
    HashMap<String, Object> location = new HashMap<String, Object>() ;
    location.put("lat",  40.73d) ;
    location.put("lon",  -74.1d) ;
    record.put("location", location) ;
    
    return record;
  }
  
  static public class Record extends HashMap<String, Object> {
  }
}
