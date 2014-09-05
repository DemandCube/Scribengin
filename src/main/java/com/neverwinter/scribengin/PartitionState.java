package com.neverwinter.scribengin;

import java.util.Collections;
import java.util.Set;

public class PartitionState {
  int controller_epoch;
  Set<Integer> isr;
  int leader;
  int leader_epoch;
  int version;

  public PartitionState() {
    super();
    isr = Collections.emptySet();
  }

  public int getController_epoch() {
    return controller_epoch;
  }

  public void setController_epoch(int controller_epoch) {
    this.controller_epoch = controller_epoch;
  }

  public Set<Integer> getIsr() {
    return isr;
  }

  public void setIsr(Set<Integer> isr) {
    this.isr = isr;
  }

  public int getLeader() {
    return leader;
  }

  public void setLeader(int leader) {
    this.leader = leader;
  }

  public int getLeader_epoch() {
    return leader_epoch;
  }

  public void setLeader_epoch(int leader_epoch) {
    this.leader_epoch = leader_epoch;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  @Override
  public String toString() {
    return "PartitionState [controller_epoch=" + controller_epoch
        + ", isr=" + isr + ", leader=" + leader + ", leader_epoch="
        + leader_epoch + ", version=" + version + "]";
  }
}
