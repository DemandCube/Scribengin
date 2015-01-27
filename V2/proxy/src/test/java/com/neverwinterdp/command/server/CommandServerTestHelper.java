package com.neverwinterdp.command.server;

import static org.junit.Assert.assertTrue;

import java.io.File;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.VMScribenginSingleJVMUnitTest;
import com.neverwinterdp.server.zookeeper.ZookeeperServerLauncher;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Shell;

public class CommandServerTestHelper {
  protected ZookeeperServerLauncher zkServerLauncher ;
  public String commandServerFolder = "./src/test/resources/commandServer";
  public String proxyServerFolder = "./src/test/resources/commandServer";
  public String commandServerXml = commandServerFolder+"/override-web.xml";
  public String proxyServerXml = proxyServerFolder+"/override-web.xml";
  
  public static String expectedListVMResponse = 
      "Running VM\n"+
      "-----------------------------------------------------------------------------------------------------\n"+
      "ID                       Path                                   Roles               Cores   Memory   \n"+
      "-----------------------------------------------------------------------------------------------------\n"+
      "vm-master-1              /vm/allocated/vm-master-1              vm-master           1       128      \n"+
      "vm-scribengin-master-1   /vm/allocated/vm-scribengin-master-1   scribengin-master   1       128      \n"+
      "vm-scribengin-master-2   /vm/allocated/vm-scribengin-master-2   scribengin-master   1       128      \n\n";

  public static String expectedRegistryDumpResponse=
      "/\n"+
      "  scribengin\n"+
      "    master\n"+
      "      leader - {\"path\":\"/vm/allocated/vm-scribengin-master-1\"}\n"+
      "        leader-0000000000\n"+
      "        leader-0000000001\n"+
      "    dataflows\n"+
      "  vm\n"+
      "    history\n"+
      "    allocated\n"+
      "      vm-scribengin-master-1 - {\"storedPath\":\"/vm/allocated/vm-scribengin-master-1\",\"memory\":128,\"cpuCores\":\n"+
      "        status - \"RUNNING\"\n"+
      "          heartbeat\n"+
      "        commands\n"+
      "      vm-master-1 - {\"storedPath\":\"/vm/allocated/vm-master-1\",\"memory\":128,\"cpuCores\":1,\"hostname\n"+
      "        status - \"RUNNING\"\n"+
      "          heartbeat\n"+
      "        commands\n"+
      "      vm-scribengin-master-2 - {\"storedPath\":\"/vm/allocated/vm-scribengin-master-2\",\"memory\":128,\"cpuCores\":\n"+
      "        status - \"RUNNING\"\n"+
      "          heartbeat\n"+
      "        commands\n"+
      "    commandServer - http://localhost:8181\n"+
      "    master\n"+
      "      leader - {\"path\":\"/vm/allocated/vm-master-1\"}\n"+
      "        leader-0000000000\n";
  
  
  protected VMScribenginSingleJVMUnitTest clusterBuilder ;
  
  public String getCommandServerFolder() {
    return commandServerFolder;
  }

  public String getProxyServerFolder() {
    return proxyServerFolder;
  }

  public String getCommandServerXml() {
    return commandServerXml;
  }

  public String getProxyServerXml() {
    return proxyServerXml;
  }

  
  public void setup() throws Exception {
    clusterBuilder = new VMScribenginSingleJVMUnitTest();
    clusterBuilder.setup();
  }
  
  public void teardown() throws Exception {
    clusterBuilder.teardown();
  }

  protected Registry getNewRegistry() {
    return new RegistryImpl(RegistryConfig.getDefault());
  }
  
  protected Shell newShell() throws RegistryException {
    VMClient vmClient = new VMClient(new RegistryImpl(RegistryConfig.getDefault()).connect()) ;
    return new Shell(vmClient) ;
  }
  
  public void assertWebXmlFilesExist(){
    //Check that web.xml exists
    File f = new File(commandServerXml);
    assertTrue(f.exists());
    f = new File(proxyServerXml);
    assertTrue(f.exists());
  }
}
