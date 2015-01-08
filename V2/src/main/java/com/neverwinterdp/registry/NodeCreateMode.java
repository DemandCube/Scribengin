package com.neverwinterdp.registry;

public enum NodeCreateMode {
  /**
   * The node should be stored permanent in the registry database.
   */
  PERSISTENT,
  /**
  * The node will not be automatically deleted upon client's disconnect,
  * and its name will be appended with a monotonically increasing number.
  */
  PERSISTENT_SEQUENTIAL,
  /**
   * The node will be deleted upon the client's disconnect.
   */
  EPHEMERAL,
  /**
   * The node will be deleted upon the client's disconnect, and its name
   * will be appended with a monotonically increasing number.
   */
  EPHEMERAL_SEQUENTIAL;
}
