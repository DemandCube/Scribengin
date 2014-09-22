package com.neverwinterdp.scribengin.commitlog;

import java.io.IOException;

public abstract class AbstractScribeCommitLogFactory {

  public abstract ScribeCommitLog build() throws IOException;

}
