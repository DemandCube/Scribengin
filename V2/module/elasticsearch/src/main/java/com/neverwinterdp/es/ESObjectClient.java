package com.neverwinterdp.es;

import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.BaseQueryBuilder;
import org.elasticsearch.search.SearchHit;

import com.neverwinterdp.util.JSONSerializer;

public class ESObjectClient<T> {
  private ESClient esclient;
  private String   index;
  private Class<T> mappingType ;
  
  public ESObjectClient(ESClient client, String index, Class mappingType) {
    this.esclient = client;
    this.index = index;
    this.mappingType = mappingType;
  }

  public ESClient getESClient() { return this.esclient ; }
  
  public String getIndex() {
    return this.index;
  }

  public Class<T> getIDocumentType() {
    return this.mappingType;
  }
  
  public boolean isCreated() {
    return esclient.hasIndex(index) ;
  }

  public void createIndexWith(String settings, String mapping) throws Exception {
    CreateIndexRequestBuilder req = esclient.client.admin().indices().prepareCreate(index);
    if (settings != null) {
      req.setSettings(settings);
    }
    if (mapping != null) {
      req.addMapping(mappingType.getSimpleName(), mapping);
    }
    CreateIndexResponse response = req.execute().actionGet();
  }

  public void updateSettings(String settings) throws Exception {
    esclient.updateSettings(index, settings);
  }

  public void updateMapping(String mapping) throws Exception {
    esclient.updateMapping(index, mappingType.getSimpleName(), mapping);
  }

  public void put(T idoc, String id) throws ElasticsearchException{
    BulkRequestBuilder bulkRequest = esclient.client.prepareBulk();
    byte[] data = JSONSerializer.INSTANCE.toBytes(idoc);
    bulkRequest.add(
        esclient.client.prepareIndex(index, mappingType.getSimpleName(), id).
            setSource(data)
        );
    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      throw new ElasticsearchException("The operation has been failed!!!");
    }
  }

  public void put(Map<String, T> records) throws ElasticsearchException {
    BulkRequestBuilder bulkRequest = esclient.client.prepareBulk();
    for (Map.Entry<String, T> entry : records.entrySet()) {
      T idoc = entry.getValue();
      byte[] data = JSONSerializer.INSTANCE.toBytes(idoc);
      bulkRequest.add(
          esclient.client.prepareIndex(index, mappingType.getSimpleName(), entry.getKey()).
              setSource(data)
          );
    }
    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      throw new ElasticsearchException("The operation has been failed!!!");
    }
  }

  public T get(String id) throws ElasticsearchException {
    GetResponse response = esclient.client.prepareGet(index, mappingType.getSimpleName(), id).execute().actionGet();
    if (!response.isExists()) return null;
    return JSONSerializer.INSTANCE.fromBytes(response.getSourceAsBytes(), mappingType);
  }

  public boolean remove(String id) throws ElasticsearchException {
    DeleteResponse response =
        esclient.client.prepareDelete(index, mappingType.getSimpleName(), id).execute().actionGet();
    return response.isFound();
  }

  public SearchResponse search(BaseQueryBuilder xqb) throws ElasticsearchException {
    return search(xqb, false, 0, 100);
  }

  public SearchResponse search(BaseQueryBuilder xqb, int from, int to) throws ElasticsearchException {
    return search(xqb, false, from, to);
  }

  public T getIDocument(SearchHit hit) throws ElasticsearchException {
    return JSONSerializer.INSTANCE.fromBytes(hit.source(), mappingType);
  }

  public SearchResponse search(BaseQueryBuilder xqb, boolean explain, int from, int to) throws ElasticsearchException {
    SearchResponse response =
        esclient.client.prepareSearch(index).setSearchType(SearchType.QUERY_THEN_FETCH).
            setQuery(xqb).
            setFrom(from).setSize(to).
            setExplain(explain).
            execute().actionGet();
    return response;
  }

  public long count(BaseQueryBuilder xqb) throws Exception {
    CountResponse response =
        esclient.client.prepareCount(index).
            setQuery(xqb).
            execute().actionGet();
    return response.getCount();
  }
  
  public void close()  {
    esclient.close() ; 
  }
}