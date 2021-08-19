package org.apache.drill.exec.store.ipfs.formats.text.compliant;

import org.apache.drill.shaded.guava.com.google.common.base.Predicate;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Iterables;
import com.univocity.parsers.common.TextParsingException;
import io.ipfs.multihash.Multihash;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.ipfs.IPFSContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class IPFSTextRecordReader extends AbstractRecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSTextRecordReader.class);

  private static final int MAX_RECORDS_PER_BATCH = 8096;
  private static final int READ_BUFFER = 1024*1024;
  private static final int WHITE_SPACE_BUFFER = 64*1024;
  // When no named column is required, ask SCAN to return a DEFAULT column.
  // If such column does not exist, it will be returned as a nullable-int column.
  private static final List<SchemaPath> DEFAULT_NAMED_TEXT_COLS_TO_READ =
      ImmutableList.of(SchemaPath.getSimplePath("_DEFAULT_COL_TO_READ_"));

  private final Multihash target;
  private final IPFSContext ipfsContext;
  // settings to be used while parsing
  private TextParsingSettings settings;
  // text reader implementation
  private TextReader reader;
  // input buffer
  private DrillBuf readBuffer;
  // working buffer to handle whitespaces
  private DrillBuf whitespaceBuffer;
  // operator context for OutputMutator
  private OperatorContext oContext;

  public IPFSTextRecordReader(IPFSContext ipfsContext, Multihash targetHash, TextParsingSettings settings, List<SchemaPath> columns) {
    this.target = targetHash;
    this.ipfsContext = ipfsContext;
    this.settings = settings;
    setColumns(columns);

  }

  // checks to see if we are querying all columns(star) or individual columns
  @Override
  public boolean isStarQuery() {
    if(settings.isUseRepeatedVarChar()) {
      return super.isStarQuery() || Iterables.tryFind(getColumns(), new Predicate<SchemaPath>() {
        @Override
        public boolean apply(@Nullable SchemaPath path) {
          return path.equals(RepeatedVarCharOutput.COLUMNS);
        }
      }).isPresent();
    }
    return super.isStarQuery();
  }

  /**
   * Returns list of default columns to read to replace empty list of columns.
   * For text files without headers returns "columns[0]".
   * Text files with headers do not support columns syntax,
   * so when header extraction is enabled, returns fake named column "_DEFAULT_COL_TO_READ_".
   *
   * @return list of default columns to read
   */
  @Override
  protected List<SchemaPath> getDefaultColumnsToRead() {
    if (settings.isHeaderExtractionEnabled()) {
      return DEFAULT_NAMED_TEXT_COLS_TO_READ;
    }
    return DEFAULT_TEXT_COLS_TO_READ;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator outputMutator) throws ExecutionSetupException {
    oContext = context;
    // Note: DO NOT use managed buffers here. They remain in existence
    // until the fragment is shut down. The buffers here are large.
    // If we scan 1000 files, and allocate 1 MB for each, we end up
    // holding onto 1 GB of memory in managed buffers.
    // Instead, we allocate the buffers explicitly, and must free
    // them.
//    readBuffer = context.getManagedBuffer(READ_BUFFER);
//    whitespaceBuffer = context.getManagedBuffer(WHITE_SPACE_BUFFER);
    readBuffer = context.getAllocator().buffer(READ_BUFFER);
    whitespaceBuffer = context.getAllocator().buffer(WHITE_SPACE_BUFFER);

    // setup Output, Input, and Reader
    try {
      TextOutput output = null;
      TextInput input = null;
      InputStream stream = null;

      // setup Input using InputStream
      logger.trace("Reading IPFS object {}", target.toString());
      //FIXME add timeout control
      //byte[] rawDataBytes = ipfsHelper.timedFailure(ipfsHelper.getClient().object::data, target, );
      byte[] rawDataBytes = ipfsContext.getIPFSClient().object.data(target);
      stream = new SeekableByteArrayInputStream(rawDataBytes);
      input = new TextInput(settings, stream, readBuffer, 0, rawDataBytes.length);

      // setup Output using OutputMutator
      if (settings.isHeaderExtractionEnabled()){
        //extract header and use that to setup a set of VarCharVectors
        String [] fieldNames = extractHeader(new SeekableByteArrayInputStream(rawDataBytes), rawDataBytes.length);
        output = new FieldVarCharOutput(outputMutator, fieldNames, getColumns(), isStarQuery());
      } else {
        //simply use RepeatedVarCharVector
        output = new RepeatedVarCharOutput(outputMutator, getColumns(), isStarQuery());
      }

      // setup Reader using Input and Output
      reader = new TextReader(settings, input, output, whitespaceBuffer);
      reader.start();

    } catch (SchemaChangeException | IOException e) {
      throw new ExecutionSetupException(String.format("Failure while setting up text reader for IPFS object %s", target.toString()), e);
    } catch (IllegalArgumentException e) {
      throw UserException.dataReadError(e).addContext("Target hash", target.toString()).build(logger);
    }
  }

  /**
   * This method is responsible to implement logic for extracting header from text file
   * Currently it is assumed to be first line if headerExtractionEnabled is set to true
   * TODO: enhance to support more common header patterns
   * @return field name strings
   */
  @SuppressWarnings("resource")
  private String [] extractHeader(InputStream in, int length) throws SchemaChangeException, IOException, ExecutionSetupException{
    assert (settings.isHeaderExtractionEnabled());
    assert (oContext != null);

    // don't skip header in case skipFirstLine is set true
    settings.setSkipFirstLine(false);

    HeaderBuilder hOutput = new HeaderBuilder();

    // setup Input using InputStream
    // we should read file header irrespective of split given given to this reader
    InputStream hStream = in;
    TextInput hInput = new TextInput(settings,  hStream, oContext.getManagedBuffer(READ_BUFFER), 0, length);

    // setup Reader using Input and Output
    this.reader = new TextReader(settings, hInput, hOutput, oContext.getManagedBuffer(WHITE_SPACE_BUFFER));
    reader.start();

    // extract first row only
    reader.parseNext();

    // grab the field names from output
    String [] fieldNames = hOutput.getHeaders();

    // cleanup and set to skip the first line next time we read input

    reader.close();
    settings.setSkipFirstLine(true);

    return fieldNames;
  }


  /**
   * Generates the next record batch
   * @return  number of records in the batch
   *
   */
  @Override
  public int next() {
    reader.resetForNextBatch();
    int cnt = 0;

    try{
      while(cnt < MAX_RECORDS_PER_BATCH && reader.parseNext()){
        cnt++;
      }
      reader.finishBatch();
      return cnt;
    } catch (IOException | TextParsingException e) {
      throw UserException.dataReadError(e)
          .addContext("Failure while reading object %s. Happened at or shortly before byte position %d.",
              target.toString(), reader.getPos())
          .build(logger);
    }
  }

  /**
   * Cleanup state once we are finished processing all the records.
   * This would internally close the input stream we are reading from.
   */
  @Override
  public void close() {

    // Release the buffers allocated above. Double-check to handle
    // unexpected multiple calls to close().

    if (readBuffer != null) {
      readBuffer.release();
      readBuffer = null;
    }
    if (whitespaceBuffer != null) {
      whitespaceBuffer.release();
      whitespaceBuffer = null;
    }
    try {
      if (reader != null) {
        reader.close();
        reader = null;
      }
    } catch (IOException e) {
      logger.warn("Exception while closing stream.", e);
    }
  }
}
