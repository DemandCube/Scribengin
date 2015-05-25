package com.neverwinterdp.zk.tool.server;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import com.neverwinterdp.tool.server.Server;
import com.neverwinterdp.util.JSONSerializer;
/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
public class EmbededZKServer implements Server {
  private ZookeeperLaucher launcher;
  private Thread zkThread;
  private Properties zkProperties = new Properties();
  private int port;

  public EmbededZKServer(Map<String, String> overrideProperties) {
    init(overrideProperties);
  }

  public EmbededZKServer(String dataDir, int port) {
    Map<String, String> props = new HashMap<String, String>();
    props.put("dataDir", dataDir);
    this.port = port;
    props.put("clientPort", Integer.toString(port));
    init(props);
  }
  
  public EmbededZKServer(String dataDir) {
    this(dataDir, 2181);
  }

  void init(Map<String, String> overrideProperties) {
    zkProperties.put("dataDir", "./build/data/zookeeper");
    // the port at which the clients will connect
    zkProperties.put("clientPort", "2181");
    if (overrideProperties != null) {
      zkProperties.putAll(overrideProperties);
    }
    //disable the per-ip limit on the number of connections since this is a non-production config
    zkProperties.put("maxClientCnxns", "0");
  }

  public void start() throws Exception {
    if (launcher != null) {
      throw new IllegalStateException("ZookeeperLaucher should be null");
    }
    System.out.println("zookeeper config zkProperties: \n" + JSONSerializer.INSTANCE.toString(zkProperties));

    zkThread = new Thread() {
      public void run() {
        try {
          launcher = create(zkProperties);
          launcher.start();
        } catch (Exception ex) {
          launcher = null;
          System.err.println("Cannot lauch the EmbededZKServer" + ex);
          throw new RuntimeException("Cannot lauch the EmbededZKServer", ex);
        }
      }
    };
    zkThread.start();
    // wait to make sure the server is launched
    Thread.sleep(3000);
  }

  ZookeeperLaucher create(Properties zkProperties) throws ConfigException, IOException {
    QuorumPeerConfig zkConfig = new QuorumPeerConfig();
    zkConfig.parseProperties(zkProperties);
    DatadirCleanupManager purgeMgr =
        new DatadirCleanupManager(zkConfig.getDataDir(), zkConfig.getDataLogDir(),
            zkConfig.getSnapRetainCount(), zkConfig.getPurgeInterval());
    purgeMgr.start();

    if (zkConfig.getServers().size() > 0) {
      return new QuorumPeerMainExt(zkConfig);
    } else {
      System.out
          .println("Either no config or no quorum defined in config, running in standalone mode");
      // there is only server in the quorum -- run as standalone
      return new ZooKeeperServerMainExt(zkConfig);
    }
  }

  @Override
  public String getHost() { return "127.0.0.1"; }

  @Override
  public int getPort() { return this.port; }
  
  @Override
  public String getConnectString() { return getHost() + ":" + getPort() ; }

  @Override
  public void shutdown() {
    if (launcher != null) {
      launcher.shutdown();
      launcher = null;
    }
  }

  static public interface ZookeeperLaucher {
    void start() throws Exception;

    void shutdown();

    QuorumPeerConfig getConfig();
  }

  public class QuorumPeerMainExt extends QuorumPeerMain implements ZookeeperLaucher {
    private QuorumPeerConfig zkConfig;

    public QuorumPeerMainExt(QuorumPeerConfig zkConfig) {
      this.zkConfig = zkConfig;
    }

    public void start() throws Exception {
      runFromConfig(zkConfig);
    }

    public void shutdown() {
      quorumPeer.shutdown();
    }

    @Override
    public QuorumPeerConfig getConfig() {
      return zkConfig;
    }
  }
  
  public class ZooKeeperServerMainExt extends ZooKeeperServerMain implements ZookeeperLaucher {
    private QuorumPeerConfig qConfig;

    public ZooKeeperServerMainExt(QuorumPeerConfig qConfig) {
      this.qConfig = qConfig;
    }

    public void start() throws Exception {
      ServerConfig config = new ServerConfig();
      config.readFrom(qConfig);
      // ManagedUtil.registerLog4jMBeans();
      runFromConfig(config);
    }

    public void shutdown() {
      super.shutdown();
    }

    @Override
    public QuorumPeerConfig getConfig() {
      return qConfig;
    }
  }
}
