package com.neverwinterdp.scribengin;

import java.io.IOException;

public abstract class AbstractScribeCommitLogFactory {

  public abstract ScribeCommitLog build() throws IOException;

}
