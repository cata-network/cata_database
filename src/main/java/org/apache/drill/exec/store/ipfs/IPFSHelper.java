package org.apache.drill.exec.store.ipfs;

import io.ipfs.api.IPFS;
import io.ipfs.api.MerkleNode;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.ipfs.IPFSStoragePluginConfig.IPFSTimeOut;
import org.bouncycastle.util.Strings;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;



public class IPFSHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSHelper.class);

  public static final String IPFS_NULL_OBJECT_HASH = "QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n";
  public static final Multihash IPFS_NULL_OBJECT = Multihash.fromBase58(IPFS_NULL_OBJECT_HASH);

  private WeakReference<ExecutorService> executorService;
  private static ExecutorService DEFAULT_EXECUTOR = Executors.newSingleThreadExecutor();
  private IPFS client;
  private IPFSPeer myself;
  private int maxPeersPerLeaf;
  private Map<IPFSTimeOut, Integer> timeouts;

  class DefaultWeakReference<T> extends WeakReference<T> {
    private T default_;
    public DefaultWeakReference(T referent, T default_) {
      super(referent);
      this.default_ = default_;
    }

    @Override
    public T get() {
      T ret = super.get();
      if (ret == null) {
        return default_;
      } else {
        return ret;
      }
    }
  }

  public IPFSHelper(IPFS ipfs) {
    executorService = new DefaultWeakReference<>(DEFAULT_EXECUTOR, DEFAULT_EXECUTOR);
    this.client = ipfs;
  }

  public void setExecutorService(ExecutorService executorService) {
    this.executorService = new DefaultWeakReference<>(executorService, DEFAULT_EXECUTOR);
  }

  public void setTimeouts(Map<IPFSTimeOut, Integer> timeouts) {
    this.timeouts = timeouts;
  }

  public void setMyself(IPFSPeer myself) {
    this.myself = myself;
  }

  public void setMaxPeersPerLeaf(int maxPeersPerLeaf) {
    this.maxPeersPerLeaf = maxPeersPerLeaf;
  }

  public IPFS getClient() {
    return client;
  }

  public List<Multihash> findprovsTimeout(Multihash id) throws IOException {
    List<String> providers;
    providers = client.dht.findprovsListTimeout(id, maxPeersPerLeaf, timeouts.get(IPFSTimeOut.FIND_PROV), executorService.get());

    List<Multihash> ret = providers.stream().map(str -> Multihash.fromBase58(str)).collect(Collectors.toList());
    return ret;
  }

  public List<MultiAddress> findpeerTimeout(Multihash peerId) throws IOException {
    // trying to resolve addresses of a node itself will always hang
    // so we treat it specially
    if(peerId.equals(myself.getId())) {
      return myself.getMultiAddresses();
    }

    List<String> addrs;
    addrs = client.dht.findpeerListTimeout(peerId, timeouts.get(IPFSTimeOut.FIND_PEER_INFO), executorService.get());
    List<MultiAddress>
        ret = addrs
        .stream()
        .filter(addr -> !addr.equals(""))
        .map(str -> new MultiAddress(str)).collect(Collectors.toList());
    return ret;
  }

  @FunctionalInterface
  public interface ThrowingFunction<T, R, E extends Exception>{
    R apply(final T in) throws E;
  }

  @FunctionalInterface
  public interface ThrowingSupplier<R, E extends Exception> {
    R get() throws E;
  }

  /**
   * Execute a time-critical operation op within time timeout. Throws TimeoutException, so the
   * caller has a chance to recover from a timeout.
   * @param op a Function that represents the operation to perform
   * @param in the parameter for op
   * @param timeout consider the execution has timed out after this amount of time in seconds
   * @param <T>
   * @param <R>
   * @param <E>
   * @return R the result of the operation
   * @throws TimeoutException
   * @throws E
   */
  public <T, R, E extends Exception> R timed(ThrowingFunction<T, R, E> op, T in, int timeout) throws TimeoutException, E {
    Callable<R> task = () -> op.apply(in);
    Future<R> res = executorService.get().submit(task);
    try {
      return res.get(timeout, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      throw (E) e.getCause();
    } catch (CancellationException | InterruptedException e) {
      throw UserException.executionError(e).build(logger);
    }
  }

  /**
   * Execute a time-critical operation op within time timeout. Causes the query to fail completely
   * if the operation times out.
   * @param op a Function that represents the operation to perform
   * @param in the parameter for op
   * @param timeout consider the execution has timed out after this amount of time in seconds
   * @param <T>
   * @param <R>
   * @param <E>
   * @return R the result of the operation
   * @throws E
   */
  public <T, R, E extends Exception> R timedFailure(ThrowingFunction<T, R, E> op, T in, int timeout) throws E {
    Callable<R> task = () -> op.apply(in);
    return timedFailure(task, timeout, TimeUnit.SECONDS);
  }

  public <R, E extends Exception> R timedFailure(ThrowingSupplier<R, E> op, int timeout) throws E {
    Callable<R> task = op::get;
    return timedFailure(task, timeout, TimeUnit.SECONDS);
  }

  private <R, E extends Exception> R timedFailure(Callable<R> task, int timeout, TimeUnit timeUnit) throws E {
    Future<R> res = executorService.get().submit(task);
    try {
      return res.get(timeout, timeUnit);
    } catch (ExecutionException e) {
      throw (E) e.getCause();
    } catch (TimeoutException e) {
      throw UserException.executionError(e).message("IPFS operation timed out").build(logger);
    } catch (CancellationException | InterruptedException e) {
      throw UserException.executionError(e).build(logger);
    }
  }

  public static Optional<String> pickPeerHost(List<MultiAddress> peerAddrs) {
    String localAddr = null;
    for (MultiAddress addr : peerAddrs) {
      String host = addr.getHost();
      try {
        InetAddress inetAddress = InetAddress.getByName(host);
        if (inetAddress.isLoopbackAddress()) {
          continue;
        }
        if (inetAddress.isSiteLocalAddress() || inetAddress.isLinkLocalAddress()) {
          //FIXME we don't know which local address can be reached; maybe check with InetAddress.isReachable?
          localAddr = host;
        } else {
          return Optional.of(host);
        }
      } catch (UnknownHostException e) {
        continue;
      }
    }

    return Optional.ofNullable(localAddr);
  }

  public Optional<String> getPeerDrillHostname(Multihash peerId) {
    return getPeerData(peerId, "drill-hostname").map(Strings::fromByteArray);
  }

  public boolean isDrillReady(Multihash peerId) {
    try {
      return getPeerData(peerId, "drill-ready").isPresent();
    } catch (RuntimeException e) {
      return false;
    }
  }

  public Optional<Multihash> getIPNSDataHash(Multihash peerId) {
    Optional<List<MerkleNode>> links = getPeerLinks(peerId);
    if (!links.isPresent()) {
      return Optional.empty();
    }

    return links.get().stream()
        .filter(l -> l.name.equals(Optional.of("drill-data")))
        .findFirst()
        .map(l -> l.hash);
  }


  private Optional<byte[]> getPeerData(Multihash peerId, String key) {
    Optional<List<MerkleNode>> links = getPeerLinks(peerId);
    if (!links.isPresent()) {
      return Optional.empty();
    }

    return links.get().stream()
        .filter(l -> l.name.equals(Optional.of(key)))
        .findFirst()
        .map(l -> {
          try {
            return client.object.data(l.hash);
          } catch (IOException e) {
            return null;
          }
        });
  }

  private Optional<List<MerkleNode>> getPeerLinks(Multihash peerId) {
    try {
      Optional<String> optionalPath = client.name.resolve(peerId, 30);
      if (!optionalPath.isPresent()) {
        return Optional.empty();
      }
      String path = optionalPath.get().substring(6); // path starts with /ipfs/Qm...

      List<MerkleNode> links = client.object.get(Multihash.fromBase58(path)).links;
      if (links.size() < 1) {
        return Optional.empty();
      } else {
        return Optional.of(links);
      }
    } catch (IOException e) {
      return Optional.empty();
    }
  }
}
