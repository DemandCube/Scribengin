package com.neverwinterdp.swing.es;

import java.util.List;

import com.neverwinterdp.es.ESClient;

@SuppressWarnings("serial")
abstract public class UISearchQueryPlugin {
  public void onInit(UISearchQuery uiSearchQuery) {
  }

  abstract public String[] getColumNames();
  
  abstract public List<Object[]> query(ESClient esClient, String index, String query) throws Exception ;
}
