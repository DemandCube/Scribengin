package com.neverwinterdp.vm.command;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.neverwinterdp.util.JSONSerializer;

public class CommandResult<T> {
  private T result ;
  private boolean discardResult ;
  private String errorStacktrace;
  
  public <V> V getResultAs(Class<V> type) { return (V) result; }
  
  @JsonDeserialize(using=JSONSerializer.GenericTypeDeserializer.class)
  public T getResult() { return result; }
  
  @JsonSerialize(using=JSONSerializer.GenericTypeSerializer.class)
  public void setResult(T result) { this.result = result; }

  public boolean isDiscardResult() { return discardResult; }
  public void setDiscardResult(boolean discardResult) {
    this.discardResult = discardResult;
  }

  public String getErrorStacktrace() { return errorStacktrace; }
  public void setErrorStacktrace(String errorStacktrace) { this.errorStacktrace = errorStacktrace; }
}
