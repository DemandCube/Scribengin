package com.neverwinterdp.scribengin.task;

import org.json.CDL;
import org.json.JSONException;
import org.json.JSONObject;

import com.neverwinterdp.scribengin.tuple.Tuple;

public class JsonToCSVConverterTask implements Task{
  
  private String key;
  private int tupleCount;
  
  public JsonToCSVConverterTask(){
    this("data");
  }

  public JsonToCSVConverterTask(String key){
    this.key = key;
    this.tupleCount = 0;
  }
  
  @Override
  public Tuple[] execute(Tuple t) {
    this.tupleCount++;
    
    JSONObject json;
    Tuple[] tupleArray = new Tuple[1];
    
    String csvData="";
    try {
      json = new JSONObject(new String(t.getData()));
      csvData = CDL.toString(json.getJSONArray(key));
    } catch (JSONException e) {
      e.printStackTrace();
      t.setInvalidData(true);
      tupleArray[0] = t;
      return tupleArray;
    }
    
    if(csvData == null || csvData.isEmpty()){
      t.setInvalidData(true);
      tupleArray[0] = t;
      return tupleArray;
    }
    
    t.setData(csvData.getBytes());
    tupleArray[0] = t;
    return tupleArray;
  }

  @Override
  public boolean readyToCommit() {
    if(this.tupleCount > 9){
      return true;
    }
    
    return false;
  }

  @Override
  public boolean commit(){
    this.tupleCount = 0;
    return true;
  }
}
