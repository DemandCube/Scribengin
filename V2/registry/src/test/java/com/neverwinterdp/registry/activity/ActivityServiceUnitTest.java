package com.neverwinterdp.registry.activity;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.zk.tool.server.EmbededZKServer;

public class ActivityServiceUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  static public String ACTIVITIES_PATH = "/activities" ;
  static private EmbededZKServer zkServerLauncher ;

  private Registry registry ;
 
  
  @BeforeClass
  static public void startServer() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    zkServerLauncher = new EmbededZKServer("./build/data/zookeeper") ;
    zkServerLauncher.start();
  }
  
  static public void stopServer() throws Exception {
    zkServerLauncher.shutdown();
  }
  
  @Before
  public void setup() throws Exception {
    registry = newRegistry().connect();
  }
  
  @After
  public void teardown() throws Exception {
    registry.rdelete(ACTIVITIES_PATH);
    registry.disconnect();
  }

  private Registry newRegistry() {
    return new RegistryImpl(RegistryConfig.getDefault()) ;
  }
  
  @Test
  public void testActivityService() throws Exception {
    ActivityService service = new ActivityService(registry, ACTIVITIES_PATH) ;
    HelloActivityBuilder hello = new HelloActivityBuilder(10) ;
    
    Activity activityCreate = service.create(hello);
    
    Activity activityGet = service.getActivity(activityCreate.getId()) ;
    Assert.assertEquals(activityCreate.getId(), activityGet.getId());
    
    List<ActivityStep> activityStepsGet = service.getActivitySteps(activityCreate);
    Assert.assertEquals(10, activityStepsGet.size());
    
    ActivityStep activityStep0 = activityStepsGet.get(0) ;
    service.assign(activityCreate, activityStep0, new WorkerDescriptor());
    Assert.assertEquals(
        ActivityStep.Status.ASSIGNED,
        service.getActivityStep(activityCreate.getId(), activityStep0.getId()).getStatus());

    service.finish(activityCreate, activityStep0);
    Assert.assertEquals(
        ActivityStep.Status.FINISHED,
        service.getActivityStep(activityCreate.getId(), activityStep0.getId()).getStatus());
    
    Assert.assertEquals(1, service.getActiveActivities().size());
    registry.get("/").dump(System.out);
    
    service.history(activityGet);
    Assert.assertEquals(1, service.getHistoryActivities().size());
    registry.get("/").dump(System.out);
  }
  
  @Test
  public void testRunning() throws Exception {
    ActivityService service = new ActivityService(registry, ACTIVITIES_PATH) ;
    HelloActivityBuilder hello = new HelloActivityBuilder(10) ;
    Activity activity = service.create(hello) ;
    
    ActivityCoordinator coordinator = activity.newActivityCoordinator() ;
    coordinator.onStart(activity);
  }
  
  
  
  static public class WorkerDescriptor {
    String refPath = "some/path";

    public String getRefPath() { return refPath; }

    public void setRefPath(String refPath) { this.refPath = refPath; }
  }
}