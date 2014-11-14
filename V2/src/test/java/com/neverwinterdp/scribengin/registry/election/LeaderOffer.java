package com.neverwinterdp.scribengin.registry.election;

import java.util.Comparator;

/**
 * A leader offer is a numeric id / path pair. The id is the sequential node id
 * assigned by ZooKeeper where as the path is the absolute path to the ZNode.
 */
public class LeaderOffer {

  private Integer id;
  private String  nodePath;
  private String  hostName;

  public LeaderOffer() {
    // Default constructor
  }

  public LeaderOffer(Integer id, String nodePath, String hostName) {
    this.id = id;
    this.nodePath = nodePath;
    this.hostName = hostName;
  }

  @Override
  public String toString() {
    return "{ id:" + id + " nodePath:" + nodePath + " hostName:" + hostName
        + " }";
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getNodePath() {
    return nodePath;
  }

  public void setNodePath(String nodePath) {
    this.nodePath = nodePath;
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  /**
   * Compare two instances of {@link LeaderOffer} using only the {code}id{code} member.
   */
  public static class IdComparator implements Comparator<LeaderOffer> {

    @Override
    public int compare(LeaderOffer o1, LeaderOffer o2) {
      return o1.getId().compareTo(o2.getId());
    }
  }
}
