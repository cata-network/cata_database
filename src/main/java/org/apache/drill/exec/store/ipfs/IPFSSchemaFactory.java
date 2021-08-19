package org.apache.drill.exec.store.ipfs;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Writer;
import org.apache.drill.exec.planner.logical.CreateTableEntry;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.StorageStrategy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class IPFSSchemaFactory implements SchemaFactory{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSSchemaFactory.class);

  final String schemaName;
  final IPFSContext context;

  public IPFSSchemaFactory(IPFSContext context, String name) throws IOException {
    this.context = context;
    this.schemaName = name;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    logger.debug("registerSchemas {}", schemaName);
    IPFSTables schema = new IPFSTables(schemaName);
    SchemaPlus hPlus = parent.add(schemaName, schema);
    schema.setHolder(hPlus);
  }

  class IPFSTables extends AbstractSchema {
    private Set<String> tableNames = Sets.newHashSet();
    private final ConcurrentMap<String, Table> tables = new ConcurrentSkipListMap<>(String::compareToIgnoreCase);
    public IPFSTables (String name) {
      super(ImmutableList.<String>of(), name);
      tableNames.add(name);
    }

    public void setHolder(SchemaPlus pulsOfThis) {
    }

    @Override
    public String getTypeName() {
      return IPFSStoragePluginConfig.NAME;
    }

    @Override
    public Set<String> getTableNames() {
      return Collections.emptySet();
    }

    @Override
    public Table getTable(String tableName) {
      //TODO: better handling of table names
      logger.debug("getTable in IPFSTables {}", tableName);
      if (tableName.equals("create")) {
        return null;
      }

      IPFSScanSpec spec = new IPFSScanSpec(context, tableName);
      return tables.computeIfAbsent(name,
          n -> new DynamicDrillTable(context.getStoragePlugin(), schemaName, spec));
    }

    @Override
    public AbstractSchema getSubSchema(String name) {
      return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return Collections.emptySet();
    }

    @Override
    public CreateTableEntry createNewTable(String tableName, List<String> partitionColumns, StorageStrategy storageStrategy) {
      return createNewTable(tableName, partitionColumns);
    }

    @Override
    public CreateTableEntry createNewTable(final String tableName, List<String> partitionColumns) {
      return new CreateTableEntry(){

        @Override
        public Writer getWriter(PhysicalOperator child) throws IOException {
          return new IPFSWriter(child, tableName, context);
        }

        @Override
        public List<String> getPartitionColumns() {
          return Collections.emptyList();
        }

      };
    }

    @Override
    public boolean isMutable() {
      logger.debug("IPFS Schema isMutable called");
      return true;
    }
  }
}
