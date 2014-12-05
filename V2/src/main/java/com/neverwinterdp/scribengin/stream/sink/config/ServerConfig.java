package com.neverwinterdp.scribengin.stream.sink.config;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.neverwinterdp.util.text.StringUtil;
import com.neverwinterdp.util.text.TabularPrinter;

/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
@Singleton
public class ServerConfig {
  @Inject(optional = true) @Named("server.group")
  private String   group;
  
  @Inject(optional = true) @Named("server.name")
  private String   serverName;
  
  
  @Inject(optional = true) @Named("server.host")
  private String   host = "127.0.0.1";

  @Inject(optional = true) @Named("server.listen-port")
  private int      listenPort = 5700;

  @Inject(optional = true) @Named("server.version")
  private float    version = 1.0f;
  
  private String[] roles = {};
  
  @Inject(optional = true) @Named("server.cluster-framework")
  private String   clusterFramework ;
  
  @Inject(optional = true) @Named("server.description")
  private String   description = "a server instance";

  public String getGroup() { return group; }
  public void setGroup(String clusterName) {
    this.group = clusterName;
  }

  public String getServerName() {
    return serverName;
  }

  public void setServerName(String serverName) {
    this.serverName = serverName;
  }
  
  public String getHost() { return host;}
  public void setHost(String host) {
    this.host = host;
  }

  public float getVersion() { return version; }
  public void setVersion(float version) {
    this.version = version;
  }

  public int getListenPort() { return listenPort;}
  public void setListenPort(int listenPort) {
    this.listenPort = listenPort;
  }

  public String[] getRoles() { return roles; }
  
  @Inject(optional = true) 
  public void setRoles(@Named("server.roles") String roles) {
    this.roles = StringUtil.toStringArray(roles);
  }

  public String getClusterFramework() {
    return clusterFramework;
  }

  public void setClusterFramework(String clusterFramework) {
    this.clusterFramework = clusterFramework;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }
  
  public void dumpInfo(Appendable out) {
    int[] width = {35, 35} ;
    TabularPrinter p = new TabularPrinter(out, width) ;
    p.header("Server Config", "");
    p.row("Group", group);
    p.row("Host", host);
    p.row("Listen Port", listenPort);
    p.row("Server Version", version);
    p.row("Roles", StringUtil.join(roles, ","));
    p.row("Cluster Framework", clusterFramework);
    p.row("Description", description);
  }
}
