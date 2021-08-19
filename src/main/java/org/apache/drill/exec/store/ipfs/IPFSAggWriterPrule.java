package org.apache.drill.exec.store.ipfs;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.IPFSWriterPrel;
import org.apache.drill.exec.planner.physical.Prule;
import org.apache.drill.exec.planner.physical.UnionExchangePrel;

public class IPFSAggWriterPrule extends Prule {
  public static final RelOptRule INSTANCE = new IPFSAggWriterPrule();
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSAggWriterPrule.class);

  private IPFSAggWriterPrule() {
    //TODO: UnionExchangePrel, or ExchangePrel?
    super(RelOptHelper.some(UnionExchangePrel.class, RelOptHelper.any(IPFSWriterPrel.class)), "IPFSAggWriterPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    logger.debug("IPFSAggWriterPrule onMatch called");
    UnionExchangePrel unionExchangePrel = call.rel(0);
    IPFSWriterPrel ipfsWriterPrel = call.rel(1);
    RelNode child = ipfsWriterPrel.getInput();

    final RelNode newWriter = new IPFSWriterPrel(child.getCluster(), child.getTraitSet(), child, ipfsWriterPrel.getCreateTableEntry());
    final RelNode newUnion = new UnionExchangePrel(unionExchangePrel.getCluster(), unionExchangePrel.getTraitSet(), newWriter);
    final RelNode aggWriter = new IPFSWriterPrel(unionExchangePrel.getCluster(), unionExchangePrel.getTraitSet(), newUnion, ipfsWriterPrel.getCreateTableEntry());

    call.transformTo(aggWriter);
  }
}
