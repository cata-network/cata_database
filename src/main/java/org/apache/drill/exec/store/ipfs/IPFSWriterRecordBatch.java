package org.apache.drill.exec.store.ipfs;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.Writer;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.store.EventBasedRecordWriter;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.VarCharVector;

import java.io.IOException;

/* Write the RecordBatch to the given RecordWriter. */
public class IPFSWriterRecordBatch extends AbstractRecordBatch<Writer> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(org.apache.drill.exec.store.ipfs.IPFSWriterRecordBatch.class);

  private EventBasedRecordWriter eventBasedRecordWriter;
  private IPFSRecordWriter recordWriter;
  private long counter = 0;
  private final RecordBatch incoming;
  private boolean processed = false;
  private final String fragmentUniqueId;
  private BatchSchema schema;
  private QueryId queryId;
  private boolean isForeman;

  public IPFSWriterRecordBatch(Writer writer, RecordBatch incoming, FragmentContext context, RecordWriter recordWriter, boolean isForeman) throws OutOfMemoryException {
    super(writer, context, false);
    this.incoming = incoming;

    final ExecProtos.FragmentHandle handle = context.getHandle();
    fragmentUniqueId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    this.recordWriter = (IPFSRecordWriter) recordWriter;

    queryId = context.getHandle().getQueryId();
    this.isForeman = isForeman;
    logger.debug("IPFSWriterRecordBatch created");
  }

  @Override
  public int getRecordCount() {
    return container.getRecordCount();
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

  @Override
  public IterOutcome innerNext() {
    if (processed) {
//      cleanup();
      // if the upstream record batch is already processed and next() is called by
      // downstream then return NONE to indicate completion
      return IterOutcome.NONE;
    }

    // process the complete upstream in one next() call
    IterOutcome upstream;
    try {
      do {
        upstream = next(incoming);

        switch(upstream) {
          case OUT_OF_MEMORY:
          case STOP:
            return upstream;

          case NOT_YET:
            break;
          case NONE:
            if (schema != null) {
              // Schema is for the output batch schema which is setup in setupNewSchema(). Since the output
              // schema is fixed ((Fragment(VARCHAR), Number of records written (BIGINT)) we should set it
              // up even with 0 records for it to be reported back to the client.
              break;
            }

          case OK_NEW_SCHEMA:
            setupNewSchema();
            logger.debug("WriterRecordBatch: OK_NEW_SCHEMA");
            // $FALL-THROUGH$
          case OK:
            counter += eventBasedRecordWriter.write(incoming.getRecordCount());
            logger.debug("Total records written so far: {}", counter);

            for(final VectorWrapper<?> v : incoming) {
              v.getValueVector().clear();
            }
            break;

          default:
            throw new UnsupportedOperationException();
        }
      } while(upstream != IterOutcome.NONE);
    } catch(IOException ex) {
      logger.error("Failure during query", ex);
      kill(false);
      context.getExecutorState().fail(ex);
      return IterOutcome.STOP;
    }

    //wait for other fragment to finish, collect hash from pubsub system, then add to output
    if (isForeman) {
      //wait until all fragments' output has received from IPFS or timeout.
    } else {
      //send output to IPFS pubsub
    }
    try {
      recordWriter.finish();
      addOutputContainerData();
      processed = true;

    } catch (IOException e) {
      context.getExecutorState().fail(e);
    } finally {
      closeWriter();

      return IterOutcome.OK_NEW_SCHEMA;
    }

  }

  private void addOutputContainerData() {
    @SuppressWarnings("resource")
    final VarCharVector fragmentIdVector = (VarCharVector) container.getValueAccessorById(
        VarCharVector.class,
        container.getValueVectorId(SchemaPath.getSimplePath("Fragment")).getFieldIds())
        .getValueVector();
    AllocationHelper.allocate(fragmentIdVector, 1, 50);
    @SuppressWarnings("resource")
    final BigIntVector summaryVector = (BigIntVector) container.getValueAccessorById(BigIntVector.class,
        container.getValueVectorId(SchemaPath.getSimplePath("Number of records written")).getFieldIds())
        .getValueVector();
    AllocationHelper.allocate(summaryVector, 1, 8);
    @SuppressWarnings("resource")
    final VarCharVector partialHashVector = (VarCharVector) container.getValueAccessorById(
        VarCharVector.class,
        container.getValueVectorId(SchemaPath.getSimplePath("Partial Hash")).getFieldIds())
        .getValueVector();
    //FIXME base58 encoded hashes are 46 bytes, others may be longer
    AllocationHelper.allocate(partialHashVector, 1, 50);

    fragmentIdVector.getMutator().setSafe(0, fragmentUniqueId.getBytes());
    fragmentIdVector.getMutator().setValueCount(1);
    summaryVector.getMutator().setSafe(0, counter);
    summaryVector.getMutator().setValueCount(1);
    partialHashVector.getMutator().setSafe(0, ((IPFSJSONRecordWriter)recordWriter).getResultHash().orElse("Null").getBytes());
    partialHashVector.getMutator().setValueCount(1);

    container.setRecordCount(1);
    logger.debug("addOutputContainerData: count: {}, partial Hash: {}", counter, ((IPFSJSONRecordWriter)recordWriter).getResultHash().orElse("Null"));
  }

  protected void setupNewSchema() throws IOException {
    try {
      // update the schema in RecordWriter
      stats.startSetup();
      recordWriter.updateSchema(incoming);
      // Create three vectors for:
      //   1. Fragment unique id.
      //   2. Summary: currently contains number of records written.
      //   3. Partial Hash: hash of the part written into IPFS
      final MaterializedField fragmentIdField =
          MaterializedField.create("Fragment", Types.required(TypeProtos.MinorType.VARCHAR));
      final MaterializedField summaryField =
          MaterializedField.create("Number of records written",
              Types.required(TypeProtos.MinorType.BIGINT));
      final MaterializedField partialHashField =
          MaterializedField.create("Partial Hash",
              Types.required(TypeProtos.MinorType.VARCHAR));

      container.addOrGet(fragmentIdField);
      container.addOrGet(summaryField);
      container.addOrGet(partialHashField);
      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    } finally {
      stats.stopSetup();
    }

    eventBasedRecordWriter = new EventBasedRecordWriter(incoming, recordWriter);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    schema = container.getSchema();
  }

  /** Clean up needs to be performed before closing writer. Partially written data will be removed. */
  private void closeWriter() {
    if (recordWriter == null) {
      return;
    }

    try {
      recordWriter.cleanup();
    } catch(IOException ex) {
      context.getExecutorState().fail(ex);
    } finally {
      try {
        if (!processed) {
          recordWriter.abort();
        }
      } catch (IOException e) {
        logger.error("Abort failed. There could be leftover output files.", e);
      } finally {
        recordWriter = null;
      }
    }
  }

  @Override
  public void close() {
    closeWriter();
    super.close();
  }

  public static class IPFSWriterFragmentSummary {
    @JsonProperty
    public long writtenRecords;
    @JsonProperty
    public String fragmentId;
    @JsonProperty
    public String partialHash;
  }

  @Override
  public void dump() {
    logger.error("IPFSWriterRecordBatch[container={}, popConfig={}, counter={}, fragmentUniqueId={}, schema={}]",
        container, popConfig, counter, fragmentUniqueId, schema);
  }
}
