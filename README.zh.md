# Drill Storage Plugin for IPFS


## 目录

0. [简介](#简介)
1. [编译](#编译)
2. [安装](#安装)
3. [配置](#配置)
4. [运行](#运行)

## 简介

Minerva是Drill的存储插件，使得Drill可以轻松访问和查询存储在IPFS上的数据，如同查询一个本地的文件一般。不仅如此，Minerva不是简单地把文件从IPFS下载到本地进行查询，而是利用Drill灵活的分布式执行引擎，将查询分解并分布到其他同样运行着Minerva的节点上。这些节点存储着数据文件，在彼处执行操作将最为高效。

我们准备了一份PPT，详细介绍Minerva的工作原理：<https://www.slideshare.net/BowenDing4/minerva-ipfs-storage-plugin-for-ipfs>

架设在我们的私有集群上的Live Demo：<http://www.datahub.pub/>

该项目仍处于早期开发阶段，整体性能和稳定性不尽如人意。欢迎贡献代码！

## 编译

### 依赖

模块依赖下列项目的fork版本：

* IPFS Java API： [java-ipfs-api](https://github.com/bdchain/java-ipfs-api)

* Drill 1.16.0：[Drill-fork](https://github.com/bdchain/Drill-fork) （`1.16.0-fork`分支）

请先克隆并在本地编译安装这两个项目，否则在后续编译中会出现找不到符号的错误。

### 在Drill源码树中编译

将仓库克隆到Drill代码树的`contrib`目录下，如`contrib/storage-ipfs`：

```
cd drill/contrib/
git clone https://github.com/bdchain/Minerva.git storage-ipfs
```

编辑storage plugin模块的Parent POM (contrib/pom.xml)，在`<modules>`下添加这个插件：

```
<modules>
    <module>storage-hbase</module>
    <module>format-maprdb</module>
    .....
    <module>storage-ipfs</module>
</modules>
```

然后在Drill的代码树根目录下执行Build：

```
mvn -T 2C clean install -DskipTests　-Dcheckstyle.skip=true
```

生成的jar包在storage-ipfs/target目录下


## 安装

生成的Drill可执行文件位于`distribution/target/apache-drill-1.16.0`目录下
将整个目录复制到代码树之外，以便后续运行和测试，例如复制为`drill-run`

将生成的`drill-ipfs-storage-{version}.jar`复制到`drill-run/jars`中。

将ipfs的几个依赖包：

```
cid.jar
junit-4.12.jar
multiaddr.jar
multibase.jar
multihash.jar
hamcrest-core-1.3.jar
```

以及IPFS Java API本身编译生成的jar包`java-api-ipfs-v1.2.2.jar` 
一起复制到`drill-run/jars/3rdparty`目录下。

插件配置文件`storage-plugin-override.conf`根据需要可以复制到`drill-run/conf`文件夹下，这样插件的配置会在每次Drill启动时自动应用并启用插件，不需要手动配置。

## 配置

1. 首先设置Drill的hostname为机器的IP地址：

    编辑`conf/drill-env.sh`中的`DRILL_HOST_NAME`，改为机器的IP。
    若要在私有集群上工作的，改为内网IP，准备在公网上工作的，改为公网IP。

2. 然后配置IPFS storage plugin：

    如果没有使用`storage-plugin-override.conf`文件，需要手动配置并启用插件：
    
    到Drill的webui (<http://localhost:8047>) 的Storage 标签页下注册IPFS storage plugin，页面最下方有 New Storage Plugin 编辑框，Name 填 `ipfs`，点击Create;
    然后输入ipfs storage plugin的配置，默认配置在`storage-ipfs/src/resources/bootstrap-storage-plugins.json`
    复制中间的一段：
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
    
    `host`和`port`指定要连接的IPFS daemon的主机和端口，如IPFS没有作特别设置无需修改。
    
    `max-nodes-per-leaf`是控制每个IPFS数据块最多由多少个节点来提供，单位为秒。设置较大的值容易实现并行化，但在查询的内容在IPFS网络中分布
    较少的情况下容易导致IPFS超时；相反，设置较低的值可能导致并行化程度较低，但不易超时。
    
    `ipfs-timeouts`控制IPFS的超时时间，单位为秒。`find-provider`为查询DHT以查找某一CID的提供者的时间，`find-peer-info`是解析内容提供者的网络地址的时间，`fetch-data`是从提供者处传输内容到本地的时间。
    
    在私有集群的环境中，内容在每台节点上均匀分布的情况下，可以将`max-nodes-per-leaf`设置得大一些，`ipfs-timeouts`则可以小一些；公网环境则相反。
    
    `groupscan-worker-threads`是用于解析CID的内容提供者时并行处理的线程数目，合理数量的并行线程可以提高查询准备阶段的速度。
    
    `formats`控制读取和写入内容的格式，目前不生效。
    
    编辑完成后，点Update，然后回到Storage主页面就可以看到ipfs插件已经注册到Drill里了。
    点Enable就可以启用这个插件，在query里用ipfs的前缀可以指定使用ipfs存储引擎。
    
3. 配置IPFS
    
    首先启动IPFS daemon。设置IPFS节点的`drill-ready`标志：
    
    ```
    ipfs name publish $(\
      ipfs object patch add-link $(ipfs object new) "drill-ready" $(\
        printf "1" | ipfs object patch set-data $(ipfs object new)\
      )\
    )
    ```
    该标志位指示一台IPFS节点上运行了Drill IPFS storage plugin，可以参与Drill的分布式运行。如不设置该标志位，plugin在调度时会忽视该节点。
    
## 运行

### 嵌入式模式

启动IPFS daemon：

```
ipfs daemon &>/dev/null &
```

启动drill-embedded：

```
drill-run/bin/drill-embedded
```

即可在命令行和网页界面执行查询。

### 后台运行

如果不希望Drill占据一个终端，可以使其在后台运行。
后台运行需要借助tmux。可以通过包管理器安装。

修改systemd的服务文件`drill-embedded.service`，使其中环境变量`DRILL_HOME`指向Drill安装的路径，如：
```
Environment="DRILL_HOME=/home/drill/apache-drill-1.16.0"
```
然后安装服务文件到systemd的配置目录下，例如`/usr/lib/systemd/system`：
```
cp drill-embedded.service /usr/lib/systemd/system
```
重启systemd守护进程使配置生效：
```
systemd daemon-reload
```
启动服务：
```
systemd start drill-embedded.service
```
