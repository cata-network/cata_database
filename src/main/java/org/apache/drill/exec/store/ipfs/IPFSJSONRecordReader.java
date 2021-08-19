package org.apache.drill.exec.store.ipfs;

import com.fasterxml.jackson.core.JsonParseException;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.easy.json.JsonProcessor;
import org.apache.drill.exec.store.easy.json.reader.CountingJsonReader;
import org.apache.drill.exec.store.ipfs.IPFSStoragePluginConfig.IPFSTimeOut;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class IPFSJSONRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSJSONRecordReader.class);

  public static final long DEFAULT_ROWS_PER_BATCH = BaseValueVector.INITIAL_VALUE_ALLOCATION;

  private FragmentContext fragmentContext;
  private IPFSContext ipfsContext;
  private String subScanSpec;
  private List<SchemaPath> columnList;
  private JsonProcessor jsonReader;
  private InputStream stream;
  private int recordCount;
  private long runningRecordCount = 0;
  private final boolean enableAllTextMode;
  private final boolean enableNanInf;
  private final boolean enableEscapeAnyChar;
  private final boolean readNumbersAsDouble;
  private final boolean unionEnabled;
  private long parseErrorCount;
  private final boolean skipMalformedJSONRecords;
  private final boolean printSkippedMalformedJSONRecordLineNumber;
  private JsonProcessor.ReadState write = null;
  private VectorContainerWriter writer;

  public IPFSJSONRecordReader(FragmentContext fragmentContext, IPFSContext ipfsContext, String scanSpec, List<SchemaPath> columns) {
    this.fragmentContext = fragmentContext;
    this.ipfsContext = ipfsContext;
    this.subScanSpec = scanSpec;
    this.columnList = columns;
    setColumns(columns);
    this.fragmentContext = fragmentContext;
    // only enable all text mode if we aren't using embedded content mode.
    this.enableAllTextMode = fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR);
    this.enableEscapeAnyChar = fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_ESCAPE_ANY_CHAR_VALIDATOR);
    this.enableNanInf = fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_NAN_INF_NUMBERS_VALIDATOR);
    this.readNumbersAsDouble = fragmentContext.getOptions().getOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR);
    this.unionEnabled = fragmentContext.getOptions().getBoolean(ExecConstants.ENABLE_UNION_TYPE_KEY);
    this.skipMalformedJSONRecords = fragmentContext.getOptions().getOption(ExecConstants.JSON_SKIP_MALFORMED_RECORDS_VALIDATOR);
    this.printSkippedMalformedJSONRecordLineNumber = fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG_VALIDATOR);

  }

  @Override
  public String toString() {
    return super.toString()
        + ", recordCount = " + recordCount
        + ", parseErrorCount = " + parseErrorCount
        + ", runningRecordCount = " + runningRecordCount + ", ...]";
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    logger.debug("IPFSJSONRecordReader setup, query {}", subScanSpec);
    Multihash rootHash = Multihash.fromBase58(subScanSpec);
    //logger.debug("I am RecordReader {}", ipfsContext.getContext().getEndpoint());
    logger.debug("rootHash={}", rootHash);

    try {
      IPFSHelper helper = ipfsContext.getIPFSHelper();
      byte[] rawDataBytes;
      if (subScanSpec.equals(IPFSHelper.IPFS_NULL_OBJECT_HASH)) {
        // An empty ipfs object, but an empty string will make Jackson ObjectMapper fail
        // so treat it specially
        rawDataBytes = "[{}]".getBytes();
      } else {
        rawDataBytes = helper.timedFailure(helper.getClient().object::data,
            rootHash, ipfsContext.getStoragePluginConfig().getIpfsTimeout(IPFSTimeOut.FETCH_DATA));
      }
      String rootJson = new String(rawDataBytes);
      int  start = rootJson.indexOf("{");
      int end = rootJson.lastIndexOf("}");
      rootJson = rootJson.substring(start,end+1);


      this.stream =  new ByteArrayInputStream(rootJson.getBytes());

      this.writer = new VectorContainerWriter(output, unionEnabled);
      if (isSkipQuery()) {
        this.jsonReader = new CountingJsonReader(fragmentContext.getManagedBuffer(), enableNanInf, enableEscapeAnyChar);
      } else {
        this.jsonReader = new JsonReader.Builder(fragmentContext.getManagedBuffer())
            .schemaPathColumns(ImmutableList.copyOf(getColumns()))
            .allTextMode(enableAllTextMode)
            .skipOuterList(true)
            .readNumbersAsDouble(readNumbersAsDouble)
            .enableNanInf(enableNanInf)
            .build();
      }
      jsonReader.setSource(stream);
      jsonReader.setIgnoreJSONParseErrors(skipMalformedJSONRecords);

    } catch (final Exception e) {
      handleAndRaise("Failure reading JSON input", e);
    }

  }

  protected void handleAndRaise(String suffix, Exception e) throws UserException {

    String message = e.getMessage();
    int columnNr = -1;

    if (e instanceof JsonParseException) {
      final JsonParseException ex = (JsonParseException) e;
      message = ex.getOriginalMessage();
      columnNr = ex.getLocation().getColumnNr();
    }

    UserException.Builder exceptionBuilder = UserException.dataReadError(e)
        .message("%s - %s", suffix, message);
    if (columnNr > 0) {
      exceptionBuilder.pushContext("Column ", columnNr);
    }

    exceptionBuilder.pushContext("Record ", currentRecordNumberInFile())
        .pushContext("Hash ", subScanSpec);

    throw exceptionBuilder.build(logger);
  }

  private long currentRecordNumberInFile() {
    return runningRecordCount + recordCount + 1;
  }

  @Override
  public int next() {
    //logger.debug("I am IPFSJSONRecordReader {} calling next", ipfsContext.getContext().getEndpoint());
    writer.allocate();
    writer.reset();
    recordCount = 0;
    parseErrorCount = 0;
    if(write == JsonProcessor.ReadState.JSON_RECORD_PARSE_EOF_ERROR){
      return recordCount;
    }
    outside: while(recordCount < DEFAULT_ROWS_PER_BATCH){
      try{
        writer.setPosition(recordCount);
        write = jsonReader.write(writer);
        if(write == JsonProcessor.ReadState.WRITE_SUCCEED){
          recordCount++;
        }
        else if(write == JsonProcessor.ReadState.JSON_RECORD_PARSE_ERROR || write == JsonProcessor.ReadState.JSON_RECORD_PARSE_EOF_ERROR){
          if(skipMalformedJSONRecords == false){
            handleAndRaise("Error parsing JSON", new Exception(subScanSpec + " : line nos :" + (recordCount+1)));
          }
          ++parseErrorCount;
          if(printSkippedMalformedJSONRecordLineNumber){
            logger.debug("Error parsing JSON in " + subScanSpec + " : line nos :" + (recordCount+parseErrorCount));
          }
          if(write == JsonProcessor.ReadState.JSON_RECORD_PARSE_EOF_ERROR){
            break outside;
          }
        }
        else{
          break outside;
        }
      }
      catch(IOException ex)
      {
        handleAndRaise("Error parsing JSON", ex);
      }
    }
    // Skip empty json file with 0 row.
    // Only when data source has > 0 row, ensure the batch has one field.
    if (recordCount > 0) {
      jsonReader.ensureAtLeastOneField(writer);
    }
    writer.setValueCount(recordCount);
    updateRunningCount();
    return recordCount;
  }

  private void updateRunningCount() {
    runningRecordCount += recordCount;
  }

  @Override
  public void close() throws Exception{
    if(stream != null) {
      stream.close();
    }
    logger.debug("IPFSJSONRecordReader close");
  }
}
