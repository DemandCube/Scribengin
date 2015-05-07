package com.neverwinterdp.swing.console.robot;

import java.awt.Frame;
import java.util.ArrayList;
import java.util.List;

public class Robot {
  private Frame frame ; 
  private List<Action> actions = new ArrayList<Action>() ;
  private long animationSpeed = 1000 ;
  
  public Robot(Frame frame) {
    this.frame = frame ;
  }
  
  public long getAnimationSpeed() { return this.animationSpeed ; }
  public void setAnimationSpeed(long speed) { this.animationSpeed = speed ; }
  
  public Robot add(Action action) {
    actions.add(action) ;
    return this ;
  }
  
  public void run() throws Exception {
    FrameUI frameui = new FrameUI(frame) ;
    try {
      for(int i = 0; i < actions.size(); i++) {
        Thread.sleep(animationSpeed) ;
        Action action = actions.get(i) ;
        action.execute(frameui) ;
      }
    } finally {
      frameui.destroy() ;
    }
  }
  
  public void destroy() {
  }
}