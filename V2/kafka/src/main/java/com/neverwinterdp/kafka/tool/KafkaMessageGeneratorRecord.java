package com.neverwinterdp.kafka.tool;
import com.neverwinterdp.util.JSONSerializer;


public class KafkaMessageGeneratorRecord implements KafkaMessageGenerator{
  public byte[] nextMessage(int partition, int messageSize) {
    return JSONSerializer.INSTANCE.toString(new Record("", new byte[messageSize] )).getBytes();
  }
  
  
  public class Record {
    public String key ;
    public byte[] data ;
    
    public Record(String key, byte[] data) {
      this.key = key ;
      this.data = data ;
    }
  }
}
