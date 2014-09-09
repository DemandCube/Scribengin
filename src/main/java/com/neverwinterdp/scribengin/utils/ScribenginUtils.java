package com.neverwinterdp.scribengin.utils;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

public class ScribenginUtils {

  /**
   * Deserializer from JSON string in a byte array to a map.
   */
  private static ObjectReader reader;
  static ObjectMapper mapper = new ObjectMapper();
  static {
  }
  private static final Logger logger = Logger.getLogger(ScribenginUtils.class);

  public static String getZookeeperServers(Properties props) {
    String servers = "";
    for (Object key : props.keySet()) {
      if (((String) key).startsWith("zookeeper.server")) {
        logger.debug("Key " + key + " value "
            + props.getProperty((String) key));
        servers = servers.concat(props.getProperty((String) key))
            .concat(",");
      }
    }
    return servers;
  }

  /**
   * Convert a byte array containing a JSON string to a map of key/value
   * pairs.
   * 
   * @param bytes
   *            byte array containing the key/value pair strings
   * 
   * @return a new map instance containing the key/value pairs
   */
  public static Map<String, String> toMap(byte[] bytes) {

    reader = mapper.reader(Map.class);

    if (bytes == null || bytes.length == 0) {
      return Collections.emptyMap();
    }
    try {
      return reader.readValue(bytes);
    } catch (Exception e) {
      String contents;
      try {
        contents = new String(bytes, "UTF-8");
      } catch (UnsupportedEncodingException uue) {
        contents = "Could not read content due to " + uue;
      }
      throw new RuntimeException(
          "Error parsing JSON string: " + contents, e);
    }
  }

  public static <T> T toClass(byte[] data, Class<T> clazz) {

    reader = mapper.reader(clazz);
    if (data == null || data.length == 0) {
      return null;
    }
    try {
      return reader.readValue(data);
    } catch (Exception e) {
      String contents;
      try {
        contents = new String(data, "UTF-8");
      } catch (UnsupportedEncodingException uue) {
        contents = "Could not read content due to " + uue;
      }
      throw new RuntimeException(
          "Error parsing JSON string: " + contents, e);
    }
  }

  public static String toJson(Object command) {

    ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
    String json = "";
    try {
      json = ow.writeValueAsString(command);
    } catch (JsonProcessingException e) {
    }
    logger.info("JSON " + json);
    return json;
  }
}
