package org.apache.drill.exec.store.ipfs;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.store.StoragePluginRegistry;

import java.io.IOException;

public class IPFSWriter extends AbstractWriter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSWriter.class);

  static final int IPFS_WRITER_VALUE = 19156;
  private final IPFSContext ipfsContext;
  private final String name;


  @JsonCreator
  public IPFSWriter(
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("name") String name,
      @JsonProperty("storageConfig") StoragePluginConfig storageConfig,
      @JacksonInject StoragePluginRegistry engineRegistry) throws IOException, ExecutionSetupException {
    super(child);
    this.ipfsContext= ((IPFSStoragePlugin) engineRegistry.getPlugin(storageConfig)).getIPFSContext();
    this.name = name;
  }


  IPFSWriter(PhysicalOperator child, String name, IPFSContext ipfsContext) {
    super(child);
    this.name = name;
    this.ipfsContext = ipfsContext;
  }

  @Override
  public int getOperatorType() {
    return IPFS_WRITER_VALUE;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new IPFSWriter(child, name, ipfsContext);
  }

  public String getName() {
    return name;
  }

  public IPFSStoragePluginConfig getStorageConfig() {
    return ipfsContext.getStoragePluginConfig();
  }

  @JsonIgnore
  public IPFSContext getIPFSContext() {
    return ipfsContext;
  }
}
