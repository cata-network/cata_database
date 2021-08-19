package org.apache.drill.exec.store.ipfs;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import io.ipfs.api.IPFS;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class IPFSStoragePlugin extends AbstractStoragePlugin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSStoragePlugin.class);

  private final IPFSContext ipfsContext;
  private final IPFSStoragePluginConfig pluginConfig;
  private final IPFSSchemaFactory schemaFactory;
  private final IPFS ipfsClient;

  public IPFSStoragePlugin(IPFSStoragePluginConfig config, DrillbitContext context, String name) throws IOException {
    super(context, name);
    this.ipfsClient = new IPFS(config.getHost(), config.getPort());
    this.ipfsContext = new IPFSContext(config, this, ipfsClient);
    this.schemaFactory = new IPFSSchemaFactory(this.ipfsContext, name);
    this.pluginConfig = config;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public boolean supportsWrite() {
    return true;
  }

  @Override
  public IPFSGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    logger.debug("IPFSStoragePlugin before getPhysicalScan");
    IPFSScanSpec spec = selection.getListWith(new ObjectMapper(), new TypeReference<IPFSScanSpec>() {});
    logger.debug("IPFSStoragePlugin getPhysicalScan with selection {}", selection);
    return new IPFSGroupScan(ipfsContext, spec, null);
  }

  @Override
  public IPFSGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns) throws IOException {
    logger.debug("IPFSStoragePlugin before getPhysicalScan");
    IPFSScanSpec spec = selection.getListWith(new ObjectMapper(), new TypeReference<IPFSScanSpec>() {});
    logger.debug("IPFSStoragePlugin getPhysicalScan with selection {}, columns {}", selection, columns);
    return new IPFSGroupScan(ipfsContext, spec, columns);
  }

  @Override
  public Set<? extends RelOptRule> getOptimizerRules(OptimizerRulesContext optimizerContext, PlannerPhase phase) {
    switch (phase) {
      case PHYSICAL:
        return ImmutableSet.of(IPFSAggWriterPrule.INSTANCE);
      default:
        return ImmutableSet.of();
    }
  }

  public IPFS getIPFSClient() {
    return ipfsClient;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public IPFSStoragePluginConfig getConfig() {
    return pluginConfig;
  }

  public IPFSContext getIPFSContext() {
    return ipfsContext;
  }

}
