package com.neverwinterdp.es.cluster;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.neverwinterdp.server.RuntimeEnvironment;
import com.neverwinterdp.server.module.ModuleProperties;
import com.neverwinterdp.server.service.AbstractService;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.LoggerFactory;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class ElasticSearchClusterService extends AbstractService {
  private Logger logger ;
  
  @Inject
  private RuntimeEnvironment rtEnv ;
  
  @Inject
  private ModuleProperties moduleProperties; 
  
  @Inject(optional = true) @Named("esProperties")
  private Map<String, String> esProperties ;
  
  private Node server ;
  
  @Inject
  public void init(LoggerFactory factory) {
    logger = factory.getLogger(getClass()) ;
  }
  
  public void start() throws Exception {
    Map<String, String> properties = new HashMap<String, String>() ;
    properties.put("cluster.name", "neverwinterdp");
    properties.put("path.data", rtEnv.getDataDir() + "/elasticsearch");
    logger.info(
        "ElasticSearch default properties:\n" + 
        JSONSerializer.INSTANCE.toString(properties)
    );
    if(esProperties != null) {
      properties.putAll(esProperties);
      logger.info(
          "ElasticSearch overrided properties:\n" + 
          JSONSerializer.INSTANCE.toString(properties)
      );
    }
    
    if(moduleProperties.isDataDrop()) {
      String dataDir = properties.get("path.data") ;
      FileUtil.removeIfExist(dataDir, false);
      logger.info("module.data.drop = true, clean data directory");
    }
    
    NodeBuilder nb = nodeBuilder();
    for(Map.Entry<String, String> entry : properties.entrySet()) {
      nb.getSettings().put(entry.getKey(), entry.getValue());
    }
    server = nb.node();
  }

  public void stop() {
    server.close();
  }
}