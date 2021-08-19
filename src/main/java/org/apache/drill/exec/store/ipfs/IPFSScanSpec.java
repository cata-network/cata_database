package org.apache.drill.exec.store.ipfs;


import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.StoragePluginRegistry;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.drill.exec.store.ipfs.IPFSStoragePluginConfig.IPFSTimeOut.FIND_PEER_INFO;

@JsonTypeName("IPFSScanSpec")
public class IPFSScanSpec {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSScanSpec.class);

  public enum Prefix {
    @JsonProperty("ipfs")
    IPFS("ipfs"),
    @JsonProperty("ipns")
    IPNS("ipns");

    @JsonProperty("prefix")
    private String name;
    Prefix(String prefix) {
      this.name = prefix;
    }

    @Override
    public String toString() {
      return this.name;
    }

    @JsonCreator
    public static Prefix of(String what) {
      switch (what) {
        case "ipfs" :
          return IPFS;
        case "ipns":
          return IPNS;
        default:
          throw new InvalidParameterException("Unsupported prefix: " + what);
      }
    }
  }

  public enum Format {
    @JsonProperty("json")
    JSON("json"),
    @JsonProperty("csv")
    CSV("csv");

    @JsonProperty("format")
    private String name;
    Format(String prefix) {
      this.name = prefix;
    }

    @Override
    public String toString() {
      return this.name;
    }

    @JsonCreator
    public static Format of(String what) {
      switch (what) {
        case "json" :
          return JSON;
        case "csv":
          return CSV;
        default:
          throw new InvalidParameterException("Unsupported format: " + what);
      }
    }
  }

  public static Set<String> formats = ImmutableSet.of("json", "csv");
  private Prefix prefix;
  private String path;
  private Format formatExtension;
  private IPFSContext ipfsContext;

  @JsonCreator
  public IPFSScanSpec (@JacksonInject StoragePluginRegistry registry,
                       @JsonProperty("IPFSStoragePluginConfig") IPFSStoragePluginConfig ipfsStoragePluginConfig,
                       @JsonProperty("prefix") Prefix prefix,
                       @JsonProperty("format") Format format,
                       @JsonProperty("path") String path) throws ExecutionSetupException {
    this.ipfsContext = ((IPFSStoragePlugin) registry.getPlugin(ipfsStoragePluginConfig)).getIPFSContext();
    this.prefix = prefix;
    this.formatExtension = format;
    this.path = path;
  }

  public IPFSScanSpec (IPFSContext ipfsContext, String path) {
    this.ipfsContext = ipfsContext;
    parsePath(path);
  }

  private void parsePath(String path) {
    //FIXME: IPFS hashes are actually Base58 encoded, so "0" "O" "I" "l" are not valid
    //also CIDs can be encoded with different encodings, not necessarily Base58
    Pattern tableNamePattern = Pattern.compile("^/(ipfs|ipns)/([A-Za-z0-9]{46}(/[^#]+)*)(?:#(\\w+))?$");
    Matcher matcher = tableNamePattern.matcher(path);
    if (!matcher.matches()) {
      throw UserException.validationError().message("Invalid IPFS path in query string. Use paths of pattern `/scheme/hashpath#format`, where scheme:= \"ipfs\"|\"ipns\", hashpath:= HASH [\"/\" path], HASH is IPFS Base58 encoded hash, path:= TEXT [\"/\" path], format:= \"json\"|\"csv\"").build(logger);
    } else {
      String prefix = matcher.group(1);
      String hashPath = matcher.group(2);
      String formatExtension = matcher.group(4);
      if (formatExtension == null) {
        formatExtension = "_FORMAT_OMITTED_";
      }

      logger.debug("prefix {}, hashPath {}, format {}", prefix, hashPath, formatExtension);

      this.path = hashPath;
      this.prefix = Prefix.of(prefix);
      try {
        this.formatExtension = Format.of(formatExtension);
      } catch (InvalidParameterException e) {
        //if format is omitted or not valid, try resolve it from file extension in the path
        Pattern fileExtensionPattern = Pattern.compile("^.*\\.(\\w+)$");
        Matcher fileExtensionMatcher = fileExtensionPattern.matcher(hashPath);
        if (fileExtensionMatcher.matches()) {
          this.formatExtension = Format.of(fileExtensionMatcher.group(1));
          logger.debug("extracted format from query: {}", this.formatExtension);
        } else {
          logger.debug("failed to extract format from path: {}", hashPath);
          throw UserException.validationError().message("File format is missing and cannot be extracted from query: %s. Please specify file format explicitly by appending `#csv` or `#json`, etc, to the IPFS path.", hashPath).build(logger);
        }
      }
    }
  }

  @JsonProperty
  public Multihash getTargetHash(IPFSHelper helper) {
    try {
      Map<String, String> result = (Map<String, String>) helper.timedFailure(
          (List args) -> helper.getClient().resolve((String) args.get(0), (String) args.get(1), true),
          ImmutableList.of(prefix.toString(), path),
          ipfsContext.getStoragePluginConfig().getIpfsTimeout(FIND_PEER_INFO)
      );
      String topHashString;
      if (result.containsKey("Path")) {
        topHashString = result.get("Path");
      } else {
        throw UserException.validationError().message("Non-existent IPFS path: %s", toString()).build(logger);
      }
      topHashString = result.get("Path");
      // returns in form of /ipfs/Qma...
      Multihash topHash = Multihash.fromBase58(topHashString.split("/")[2]);
      return topHash;
    } catch (IOException e) {
      throw UserException.executionError(e).message("Unable to resolve IPFS path; is it a valid IPFS path?").build(logger);
    }
  }

  @JsonProperty
  public Prefix getPrefix() {
    return prefix;
  }

  @JsonProperty
  public Format getFormatExtension() {
    return formatExtension;
  }

  @JsonIgnore
  public IPFSContext getIPFSContext() {
    return ipfsContext;
  }

  @JsonProperty("IPFSStoragePluginConfig")
  public IPFSStoragePluginConfig getIPFSStoragePluginConfig() {
    return ipfsContext.getStoragePluginConfig();
  }

  @Override
  public String toString() {
    return "IPFSScanSpec [/" + prefix + "/" + path + "#" + formatExtension + " ]";
  }
}
