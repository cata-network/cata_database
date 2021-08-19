package org.apache.drill.exec.store.ipfs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfigBase;

import java.security.InvalidParameterException;
import java.util.Map;

@JsonTypeName(IPFSStoragePluginConfig.NAME)
public class IPFSStoragePluginConfig extends StoragePluginConfigBase{
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSStoragePluginConfig.class);

    public static final String NAME = "ipfs";

    private final String host;
    private final int port;

    @JsonProperty("max-nodes-per-leaf")
    private final int maxNodesPerLeaf;

    //TODO add more specific timeout configs fot different operations in IPFS,
    // eg. provider resolution, data read, etc.
    @JsonProperty("ipfs-timeouts")
    private final Map<IPFSTimeOut, Integer> ipfsTimeouts;

    @JsonIgnore
    private static final Map<IPFSTimeOut, Integer> ipfsTimeoutDefaults = ImmutableMap.of(
        IPFSTimeOut.FIND_PROV, 4,
        IPFSTimeOut.FIND_PEER_INFO, 4,
        IPFSTimeOut.FETCH_DATA, 6
    );

    public enum IPFSTimeOut {
        @JsonProperty("find-provider")
        FIND_PROV("find-provider"),
        @JsonProperty("find-peer-info")
        FIND_PEER_INFO("find-peer-info"),
        @JsonProperty("fetch-data")
        FETCH_DATA("fetch-data");

        @JsonProperty("type")
        private String which;
        IPFSTimeOut(String which) {
            this.which = which;
        }

        @JsonCreator
        public static IPFSTimeOut of(String which) {
            switch (which) {
                case "find-provider":
                    return FIND_PROV;
                case "find-peer-info":
                    return FIND_PEER_INFO;
                case "fetch-data":
                    return FETCH_DATA;
                default:
                    throw new InvalidParameterException("Unknown key for IPFS timeout config entry: " + which);
            }
        }

        @Override
        public String toString() {
            return this.which;
        }
    }

    @JsonProperty("groupscan-worker-threads")
    private final int numWorkerThreads;

    @JsonProperty
    private final Map<String, FormatPluginConfig> formats;

    @JsonCreator
    public IPFSStoragePluginConfig(
        @JsonProperty("host") String host,
        @JsonProperty("port") int port,
        @JsonProperty("max-nodes-per-leaf") int maxNodesPerLeaf,
        @JsonProperty("ipfs-timeouts") Map<IPFSTimeOut, Integer> ipfsTimeouts,
        @JsonProperty("groupscan-worker-threads") int numWorkerThreads,
        @JsonProperty("formats") Map<String, FormatPluginConfig> formats) {
        this.host = host;
        this.port = port;
        this.maxNodesPerLeaf = maxNodesPerLeaf > 0 ? maxNodesPerLeaf : 1;
        //TODO Jackson failed to deserialize the ipfsTimeouts map causing NPE
        if (ipfsTimeouts != null) {
            ipfsTimeoutDefaults.forEach(ipfsTimeouts::putIfAbsent);
        } else {
            ipfsTimeouts = ipfsTimeoutDefaults;
        }
        this.ipfsTimeouts = ipfsTimeouts;
        this.numWorkerThreads = numWorkerThreads > 0 ? numWorkerThreads : 1;
        this.formats = formats;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getMaxNodesPerLeaf() {
        return maxNodesPerLeaf;
    }

    public int getIpfsTimeout(IPFSTimeOut which) {
        return ipfsTimeouts.get(which);
    }

    public Map<IPFSTimeOut, Integer> getIpfsTimeouts() {
        return ipfsTimeouts;
    }

    public int getNumWorkerThreads() {
        return numWorkerThreads;
    }

    public Map<String, FormatPluginConfig> getFormats() {
        return formats;
    }

    @Override
    public int hashCode() {
        String host_port = String.format("%s:%d[%d,%s]", host, port, maxNodesPerLeaf, ipfsTimeouts);
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host_port == null) ? 0 : host_port.hashCode());
        result = prime * result + ((formats == null) ? 0 : formats.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        IPFSStoragePluginConfig other = (IPFSStoragePluginConfig) obj;
        if (formats == null) {
            if (other.formats != null) {
                return false;
            }
        } else if (!formats.equals(other.formats)) {
            return false;
        }
        if (host == null) {
            if (other.host != null) {
                return false;
            }
        } else if (!host.equals(other.host)
            || port != other.port
            || maxNodesPerLeaf != other.maxNodesPerLeaf
            || ipfsTimeouts != other.ipfsTimeouts
            || numWorkerThreads != other.numWorkerThreads ) {
            return false;
        }
        return true;
    }
}
