package org.apache.drill.exec.store.ipfs;

import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class IPFSPeer {
  private IPFSHelper helper;

  private Multihash id;
  private List<MultiAddress> addrs;
  private boolean isDrillReady;
  private boolean isDrillReadyChecked = false;
  private Optional<String> drillbitAddress = Optional.empty();
  private boolean drillbitAddressChecked = false;


  public IPFSPeer(IPFSHelper helper, Multihash id) {
    this.helper = helper;
    this.id = id;
  }

  IPFSPeer(IPFSHelper helper, Multihash id, List<MultiAddress> addrs) {
    this.helper = helper;
    this.id = id;
    this.addrs = addrs;
    this.isDrillReady = helper.isDrillReady(id);
    this.isDrillReadyChecked = true;
    this.drillbitAddress = IPFSHelper.pickPeerHost(addrs);
    this.drillbitAddressChecked = true;
  }

  public boolean isDrillReady() {
    if (!isDrillReadyChecked) {
      isDrillReady = helper.isDrillReady(id);
      isDrillReadyChecked = true;
    }
    return isDrillReady;
  }

  public boolean hasDrillbitAddress() {
    findDrillbitAddress();
    return drillbitAddress.isPresent();
  }

  public Optional<String> getDrillbitAddress() {
    findDrillbitAddress();
    return drillbitAddress;
  }

  public List<MultiAddress> getMultiAddresses() {
    findDrillbitAddress();
    return addrs;
  }

  public Multihash getId() {
    return id;
  }


  private void findDrillbitAddress() {
    if (!drillbitAddressChecked) {
      try {
        addrs = helper.findpeerTimeout(id);
        drillbitAddress = IPFSHelper.pickPeerHost(addrs);
      } catch (IOException e) {
        drillbitAddress = Optional.empty();
      }
      drillbitAddressChecked = true;
    }
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return String.format("IPFSPeer(%s)", id.toBase58());
  }

}
