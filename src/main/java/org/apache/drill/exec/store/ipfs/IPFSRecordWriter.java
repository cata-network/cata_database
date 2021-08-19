package org.apache.drill.exec.store.ipfs;

import org.apache.drill.exec.store.RecordWriter;

import java.io.IOException;

public interface IPFSRecordWriter extends RecordWriter{

  /**
   * commit written data and wrap up in a new IPFS object
   * @throws IOException
   */
  void finish() throws IOException;
}
