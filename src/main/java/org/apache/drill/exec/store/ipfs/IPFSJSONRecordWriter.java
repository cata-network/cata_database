package org.apache.drill.exec.store.ipfs;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import io.ipfs.api.IPFS;
import io.ipfs.api.MerkleNode;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.store.JSONOutputRecordWriter;
import org.apache.drill.exec.vector.complex.fn.BasicJsonOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class IPFSJSONRecordWriter extends JSONOutputRecordWriter implements IPFSRecordWriter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSJSONRecordWriter.class);
  private static final String LINE_FEED = String.format("%n");
  private static final int MAX_RECORDS_PER_LEAF = 1024;

  private IPFS ipfs;
  private final JsonFactory factory = new JsonFactory();
  private final String name;
  private ByteArrayOutputStream stream;
  private boolean fRecordStarted = false;
  private String resultHash = null;

  public IPFSJSONRecordWriter(OperatorContext context, IPFSContext ipfsContext, String name) {
    this.ipfs = ipfsContext.getIPFSClient();
    this.name = name;
    logger.debug("IPFS record writer construct, name: {}", name);
    try {
      init(null);
    } catch (IOException e) {
      //pass
    }
  }

  @Override
  public void init(Map<String, String> writerOptions) throws IOException {
    logger.debug("IPFS record writer init, writerOptions: {}", writerOptions);
    stream = new ByteArrayOutputStream();
    JsonGenerator generator = factory.createGenerator(stream).useDefaultPrettyPrinter().configure(JsonGenerator.Feature.QUOTE_NON_NUMERIC_NUMBERS, true);
    generator = generator.setPrettyPrinter(new MinimalPrettyPrinter(LINE_FEED));
    gen = new BasicJsonOutput(generator);
  }

  @Override
  public void updateSchema(VectorAccessible batch) throws IOException {
    logger.debug("IPFSJSONRecordWriter: updateSchema Called");

  }

  @Override
  public void checkForNewPartition(int index) {
    logger.debug("IPFSJSONRecordWriter: checkForNewPartition called");
    if (index > MAX_RECORDS_PER_LEAF) {
      // flush data to IPFS
    }
  }

  @Override
  public void startRecord() throws IOException {
    gen.writeStartObject();
    fRecordStarted = true;
  }

  @Override
  public void endRecord() throws IOException {
    gen.writeEndObject();
    fRecordStarted = false;
  }

  @Override
  public void abort() throws IOException {
    logger.debug("IPFS record writer abort");
    gen.flush();
    stream.reset();
  }

  /**
   * Will be called at the end of every schema write
   * Flush data to IPFS
   * @throws IOException
   */
  @Override
  public void cleanup() throws IOException {
    logger.debug("IPFS record writer cleanup");
    gen.flush();
    stream.close();
  }

  @Override
  public void finish() throws IOException {
    logger.debug("IPFSJSONRecordWriter: finish Called");
    gen.flush();
    logger.debug("IPFSJSONRecordWriter: stream.size(): {}", stream.size());
    if (stream.size() > 0) {
      //TODO add timeout
      MerkleNode node = ipfs.object.patch(
          IPFSHelper.IPFS_NULL_OBJECT,
          "set-data",
          Optional.of(stream.toByteArray()),
          Optional.empty(),
          Optional.empty()
      );
      resultHash = node.hash.toBase58();
      logger.debug("successfully written to IPFS, resulting node hash {}", node.hash);
    }
  }

  public Optional<String> getResultHash() {
    return Optional.ofNullable(resultHash);
  }
}
