package org.apache.drill.exec.store.ipfs;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

public class IPFSWriterBatchCreator implements BatchCreator<IPFSWriter> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSWriterBatchCreator.class);

  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context, IPFSWriter config, List<RecordBatch> children)
      throws ExecutionSetupException {
    assert children != null && children.size() == 1;
    boolean amIForeman = context.getForemanEndpoint().getAddress().equals(context.getEndpoint().getAddress());
    return new IPFSWriterRecordBatch(
      config,
      children.iterator().next(),
      context,
      new IPFSJSONRecordWriter(
        null,
        config.getIPFSContext(),
        config.getName()
      ),
      amIForeman
    );
  }
}
