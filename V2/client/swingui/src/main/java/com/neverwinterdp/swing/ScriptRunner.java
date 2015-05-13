package com.neverwinterdp.swing;

import java.util.HashSet;
import java.util.Map;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import com.neverwinterdp.swing.util.IOUtil;

public class ScriptRunner {
  private String baseDir ;
  private HashSet<String> loadedFiles = new HashSet<String> () ;
  private ScriptEngine engine ;
  
  
  public ScriptRunner(String baseDir, Map<String, Object> ctx) {
    this.baseDir = baseDir ;
    ScriptEngineManager factory = new ScriptEngineManager();
    engine = factory.getEngineByName("JavaScript");
    engine.put("ScriptRunner", this) ;
    if(ctx != null) {
      for(Map.Entry<String, Object> entry : ctx.entrySet()) {
        engine.put(entry.getKey(), entry.getValue());
      }
    }
  }

  public Object eval(String script) throws Exception {
    Object ret = engine.eval(script);
    return ret;
  }
  
  public void require(String scriptFile) {
    if(loadedFiles.contains(scriptFile)) return ;
    try {
      String script = null ;
      if(scriptFile.startsWith("classpath:") || scriptFile.startsWith("file:")) {
        script = IOUtil.getStreamContentAsString(IOUtil.loadRes(scriptFile), "UTF-8") ;
      } else {
        script = IOUtil.getFileContentAsString(baseDir + "/" + scriptFile, "UTF-8") ;
      }
      loadedFiles.add(scriptFile) ;
      engine.eval(script);
    } catch(Exception ex) {
      ex.printStackTrace() ;
      throw new RuntimeException(ex) ;
    }
  }
}