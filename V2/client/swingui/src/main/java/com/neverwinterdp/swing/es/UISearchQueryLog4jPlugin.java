package com.neverwinterdp.swing.es;

import java.util.ArrayList;
import java.util.List;

import javax.swing.table.TableColumnModel;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.jdesktop.swingx.JXTable;

import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.es.log4j.Log4jRecord;
import com.neverwinterdp.util.text.DateUtil;

@SuppressWarnings("serial")
public class UISearchQueryLog4jPlugin extends UISearchQueryPlugin {
  static String[] COLUMNS = {
    "Id", "Timestamp", "Level", "Thread", 
    "Logger Name", "Message"
  };
  
  public void onInit(UISearchQuery uiSearchQuery) {
    JXTable result = uiSearchQuery.queryResultPanel; 
    TableColumnModel cModel = result.getColumnModel();
    cModel.getColumn(0).setMaxWidth(30);
    cModel.getColumn(1).setMaxWidth(150);
    cModel.getColumn(2).setMaxWidth(40);
  }

  public String[] getColumNames() { return COLUMNS ; }
  
  public List<Object[]> query(ESClient esClient, String index, String query) throws Exception {
    ESObjectClient<Log4jRecord> esObjectClient = new ESObjectClient<Log4jRecord>(esClient, index, Log4jRecord.class) ;
    SearchResponse response = esObjectClient.search(QueryBuilders.queryString(query));
    List<Object[]> rows = new ArrayList<>();
    SearchHits hits = response.getHits();
    int idx = 0 ;
    for(SearchHit hit : hits.getHits()) {
      Log4jRecord record = esObjectClient.getIDocument(hit) ;
      Object[] cells = {
          idx++, DateUtil.asCompactDateTime(record.getTimestamp()), record.getLevel(), record.getThreadName(), 
          record.getLoggerName(), record.getMessage() 
      };
      rows.add(cells);
    }
    return rows ;
  }
}
