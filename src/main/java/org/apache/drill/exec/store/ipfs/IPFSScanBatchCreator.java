package org.apache.drill.exec.store.ipfs;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.ipfs.formats.text.compliant.IPFSTextRecordReader;
import org.apache.drill.exec.store.ipfs.formats.text.compliant.TextFormatConfig;
import org.apache.drill.exec.store.ipfs.formats.text.compliant.TextParsingSettings;

import java.util.LinkedList;
import java.util.List;

public class IPFSScanBatchCreator implements BatchCreator<IPFSSubScan> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSScanBatchCreator.class);

  @Override
  public ScanBatch getBatch(ExecutorFragmentContext context, IPFSSubScan subScan, List<RecordBatch> children)
      throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    List<RecordReader> readers = new LinkedList<>();
    List<SchemaPath> columns = null;
    logger.debug(String.format("subScanSpecList.size = %d", subScan.getIPFSSubScanSpecList().size()));

    for (Multihash scanSpec : subScan.getIPFSSubScanSpecList()) {
      try {
        //FIXME what are columns and what are they for?
        if ((columns = subScan.getColumns())==null) {
          columns = GroupScan.ALL_COLUMNS;
        }
        RecordReader reader;
        if (subScan.getFormat() == IPFSScanSpec.Format.JSON) {
          reader = new IPFSJSONRecordReader(context, subScan.getIPFSContext(), scanSpec.toString(), columns);
        } else {
          TextParsingSettings settings = new TextParsingSettings();
          settings.set(new TextFormatConfig());
          //TODO: set this according to whether the leaf contains the header line
          settings.setHeaderExtractionEnabled(true);
          reader = new IPFSTextRecordReader(subScan.getIPFSContext(), scanSpec, settings, columns);
        }
        readers.add(reader);
      } catch (Exception e1) {
        throw new ExecutionSetupException(e1);
      }
    }
    return new ScanBatch(subScan, context, readers);
  }
}
