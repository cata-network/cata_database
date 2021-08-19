# Drill Storage Plugin for IPFS

[中文](README.zh.md)

## Contents

0. [Introduction](#Introduction)
1. [Compile](#Compile)
2. [Install](#Install)
2. [Configuration](#Configuration)
3. [Run](#Run)

## Introduction

Minerva is a storage plugin of Drill that connects IPFS's decentralized storage and Drill's flexible query engine. Any data file stored on IPFS can be easily accessed from Drill's query interface, just like a file stored on a local disk. Moreover, with Drill's capability of distributed execution, other instances who are also running Minerva can help accelerate the execution: the data stays where it was, and the queries go to the most suitable nodes which stores the data locally and from there the operations can be performed most efficiently. 

Slides that explain our ideas and the technical details of Minerva: <https://www.slideshare.net/BowenDing4/minerva-ipfs-storage-plugin-for-ipfs>

A live demo: <http://www.datahub.pub/> hosted on a private cluster of Minerva.

Note that it's still in early stages of development and the overall stability and performance is not satisfactory. PRs are very much welcome!

## Compile

### Dependencies

This project depends on forks of the following projects:

* IPFS Java API： [java-ipfs-api](https://github.com/bdchain/java-ipfs-api)

* Drill 1.16.0：[Drill-fork](https://github.com/bdchain/Drill-fork) （`1.16.0-fork` branch）

Please clone and build these projects locally, or the compiler will complain about unknown symbols when you compile this project.

### Compile under the Drill source tree

Clone to the `contrib` directory in Drill source tree, e.g. `contrib/storage-ipfs`:
```
cd drill/contrib/
git clone https://github.com/bdchain/Minerva.git storage-ipfs
```

Edit the parent POM of Drill contrib module (contrib/pom.xml), add this plugin under `<modules>` section:

```
<modules>
    <module>storage-hbase</module>
    <module>format-maprdb</module>
    .....
    <module>storage-ipfs</module>
</modules>
```

Build from the root directory of Drill source tree:

```
mvn -T 2C clean install -DskipTests　-Dcheckstyle.skip=true
```

The jars are in the `storage-ipfs/target` directory.

## Install

The executables and configurations are in `distribution/target/apache-drill-1.16.0`. Copy the entire directory to somewhere outside the source tree, and name it `drill-run` e.g., for testing later.

Copy the `drill-ipfs-storage-{version}.jar` generated jar file to `drill-run/jars`.

Copy `java-api-ipfs-v1.2.2.jar` which is IPFS's Java API, along with its dependencies provided as jar files:

```
cid.jar
junit-4.12.jar
multiaddr.jar
multibase.jar
multihash.jar
hamcrest-core-1.3.jar
```

to `drill-run/jars/3rdparty`.

Optionally, copy the configuration override file `storage-plugin-override.conf` to `drill-run/conf`, if you want Drill to auto configure and enable IPFS storage plugin at every (re)start.

## Configuration

1. Set Drill hostname to the IP address of the node to run Drill:
    
    Edit file `conf/drill-env.sh` and change the environment variable `DRILL_HOST_NAME` to the IP address of the node. Use private or global addresses, depending on whether you plan to run it on a cluster or the open Internet.

2. Configure the IPFS storage plugin:
    
    If you are not using the configuration override file, you will have to manually configure and enable the plugin.
    
    Run Drill according to [Section Run](#Run) and go to the webui of Drill (can be found at <http://localhost:8047>). Under the Storage tab, create a new storage plugin named `ipfs` and click the Create button.
    
    Copy and paste the default configuration of the IPFS storage plugin located at `storage-ipfs/src/resources/bootstrap-storage-plugins.json`:
    
    ```
    ipfs : {
        "type":"ipfs",
        "host": "127.0.0.1",
        "port": 5001,
        "max-nodes-per-leaf": 3,
        "ipfs-timeouts": {
          "find-provider": 4,
          "find-peer-info": 4,
          "fetch-data": 5
        },
        "groupscan-worker-threads": 50,
        "formats": null,
        "enabled": true
    }
    ```
    
    where 
    
    `host` and `port` are the host and API port where your IPFS daemon will be listening. Change it so that it matches the configuration of your IPFS instance.

    `max-nodes-per-leaf` controls how many provider nodes will be considered when the query is being planned. A larger value increases the parallelization width but typically takes longer to find enough providers from DHT resolution. A smaller value does the opposite.
    
    `ipfs-timeouts` set the maximum amount of time in seconds for various time consuming operations: `find-provider` is the time allowed to do DHT queries to find providers, `find-peer-info` is the time allowed to resolve the network addresses of the providers and `fetch-data` is the time the actual transmission is allowed to take. 
    
    `groupscan-worker-threads` limits the number of worker threads when the planner communicate with IPFS daemon to resolve providers and peer info.
    
    `formats` specifies the formats of the files. It is unimplemented for now and does nothing.
    
    Click the Update button after finishing editing. You should see the IPFS storage plugin is registered with Drill and you can enable it with the Enable button.
    
3. Configure IPFS

    Start the IPFS daemon first. 
    
    Set a Drill-ready flag to the node:
    
    ```
    ipfs name publish $(\
      ipfs object patch add-link $(ipfs object new) "drill-ready" $(\
        printf "1" | ipfs object patch set-data $(ipfs object new)\
      )\
    )
    ```
    
    This flag indicates that an IPFS node is also capable of handling Drill quries and the planner will consider it when scheduling a query to execute distributedly. A node without this flag will be ignored.
    

## Run

### Embedded mode

Start IPFS daemon：

```
ipfs daemon &>/dev/null &
```

start drill-embedded：

```
drill-run/bin/drill-embedded
```

You can now execute queries via the command line as well as the web interface.

### As a background service

You can run drill-embedded as a background process without controlling a terminal. This is done with the help of tmux, which is available in many distributions of Linux.

Edit the systemd service file `drill-embedded.service`, so that the environment variable `DRILL_HOME` pointes to where Drill is installed：
```
Environment="DRILL_HOME=/home/drill/apache-drill-1.16.0"
```
Copy the service file to systemd's configuration directory, e.g. `/usr/lib/systemd/system`：
```
cp drill-embedded.service /usr/lib/systemd/system
```
Reload the systemd daemon：
```
systemd daemon-reload
```
Start the service:
```
systemd start drill-embedded.service
```
