package com.neverwinterdp.util;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * $Author: Tuan Nguyen$ 
 **/
public class JSONSerializer {
  final static public  Charset UTF8 = Charset.forName("UTF-8") ;
  final static public DateFormat COMPACT_DATE_TIME = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss 'GMT'Z")  ;
  
  final static public JSONSerializer INSTANCE = new JSONSerializer() ;

  private ObjectMapper mapper ;

  public JSONSerializer() {
    mapper = new ObjectMapper(); // can reuse, share globally
    configure(mapper) ;
  }
  
  public JSONSerializer(Module ... module) {
    this() ;
    for(Module selModule : module) {
      mapper.registerModule(selModule);
    }
  }

  public void setIgnoreUnknownProperty(boolean b) {
  }

  public <T> byte[] toBytes(T idoc)  {
    try {
      StringWriter w = new StringWriter() ;
      mapper.writeValue(w, idoc);
      w.close() ;
      return w.getBuffer().toString().getBytes(UTF8) ;
    } catch(IOException e) {
      throw new RuntimeException(e) ;
    }
  }

  public <T> T fromBytes(byte[] data, Class<T> type)  {
    try {
      StringReader reader = new StringReader(new String(data, UTF8)) ;
      return mapper.readValue(reader , type);
    } catch (IOException e) {
      throw new RuntimeException(e) ;
    }
  }
  
  public <T> T fromBytes(byte[] data, TypeReference<T> typeRef) {
    StringReader reader = new StringReader(new String(data, UTF8)) ;
    try {
      return mapper.readValue(reader , typeRef);
    } catch (IOException e) {
      throw new RuntimeException(e) ;
    }
  }

  public <T> String toString(T idoc) {
    if(idoc == null) return "" ;
    try  {
      Writer writer = new StringWriter() ;
      ObjectWriter owriter  = mapper.writerWithDefaultPrettyPrinter() ;
      owriter.writeValue(writer, idoc);
      return writer.toString() ;
    } catch(IOException ex) {
      throw new RuntimeException(ex) ;
    }
  }
  
  public <T> JsonNode toJsonNode(T idoc) throws IOException {
    return mapper.convertValue(idoc, JsonNode.class) ;
  }

  public  String toString(JsonNode node) throws IOException {
    StringWriter writer = new StringWriter() ;
    ObjectWriter owriter  = mapper.writerWithDefaultPrettyPrinter() ;
    owriter.writeValue(writer, node);
    return writer.toString() ;
  }

  public <T> T fromString(String data, Class<T> type) {
    try {
      StringReader reader = new StringReader(data) ;
      return mapper.readValue(reader , type);
    } catch (IOException e) {
      throw new RuntimeException(e) ;
    }
  }
  
  public <T> T fromString(String data, TypeReference<T> typeRef) {
    try {
      StringReader reader = new StringReader(data) ;
      return mapper.readValue(reader , typeRef);
    } catch (IOException e) {
      throw new RuntimeException(e) ;
    }
  }

  public JsonNode fromString(String data) throws IOException {
    StringReader reader = new StringReader(data) ;
    return mapper.readTree(reader);
  }
  
  public <T> T clone(T obj) {
    try  {
      Writer writer = new StringWriter() ;
      ObjectWriter owriter  = mapper.writerWithDefaultPrettyPrinter() ;
      owriter.writeValue(writer, obj);
      String json =  writer.toString() ;
      return (T) this.fromString(json, obj.getClass()) ;
    } catch(IOException ex) {
      throw new RuntimeException(ex) ;
    }
  }
  
  static public class GenericTypeDeserializer extends JsonDeserializer<Object> {
    public Object deserialize(JsonParser jsonParser, DeserializationContext ctx) throws IOException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      String resultType = node.get("type").asText();
      try {
        Class type = Class.forName(resultType)  ;
        JsonNode rnode = node.get("data") ;
        Object val = oc.treeToValue(rnode, type);
        return val ;
      } catch (ClassNotFoundException e) {
        throw new IOException(e) ;
      }
    }
  }

  static public class GenericTypeSerializer extends JsonSerializer<Object> {
    public  void serialize(Object resutl, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeObjectField("type", resutl.getClass().getName());
      jsonGenerator.writeObjectField("data", resutl);
      jsonGenerator.writeEndObject();
    }
  }
  
  static public void configure(ObjectMapper mapper) {
    mapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false) ;
    DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter() ;
    prettyPrinter.indentArraysWith(new DefaultPrettyPrinter.Lf2SpacesIndenter()) ;
    mapper.setDateFormat(COMPACT_DATE_TIME) ;
  }
}