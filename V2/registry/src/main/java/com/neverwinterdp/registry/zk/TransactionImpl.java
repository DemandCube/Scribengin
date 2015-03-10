package com.neverwinterdp.registry.zk;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;

import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;

public class TransactionImpl implements Transaction {
  private RegistryImpl registry;
  private org.apache.zookeeper.Transaction zkTransaction ;
  
  public TransactionImpl(RegistryImpl registry, org.apache.zookeeper.Transaction zkTransaction) {
    this.zkTransaction = zkTransaction;
    this.registry = registry;
  }
  
  @Override
  public Transaction create(String path, byte[] data, NodeCreateMode mode) {
    zkTransaction.create(registry.realPath(path), data, RegistryImpl.DEFAULT_ACL, RegistryImpl.toCreateMode(mode));
    return this;
  }

  @Override
  public Transaction delete(String path) {
    zkTransaction.delete(registry.realPath(path), 0);
    return this;
  }

  @Override
  public Transaction check(String path) {
    zkTransaction.check(registry.realPath(path), 0);
    return this;
  }

  @Override
  public Transaction setData(String path, byte[] data) {
    zkTransaction.setData(registry.realPath(path), data, 0);
    return this;
  }

  @Override
  public void commit() throws RegistryException {
    try {
      List<OpResult> results = zkTransaction.commit();
    } catch (InterruptedException | KeeperException e) {
      throw new RegistryException(ErrorCode.Unknown, e);
    }
  }
}
