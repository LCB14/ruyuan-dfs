
### 内容说明：
本仓库存放的是**儒猿架构**《石杉架构课》的 **自研中间件实战云平台项目之一**，版权归儒猿技术窝所有，侵权将追究法律责任

架构课程详细信息请关注公众号【儒猿技术窝】了解 - [中华石杉架构课程B站直通车](https://space.bilibili.com/478364560/channel/detail?cid=155048)

### 公众号:儒猿技术窝

更多技术干货，请扫描下方二维码，关注公众号儒猿技术窝
![](img/ruyuan.jpeg)



## 儒猿自研分布式小文件存储系统


定位: 小文件存储: 几KB~几百MB之间的文件存储。


## 特性&设计


- 改造代码为工业级项目代码，主要是调整代码结构、日志框架、代码注释补全等。
- 网络通讯技术重新选型，主要是将原本原生NIO + gRpc混合通讯的机制全部统一采用Netty进行网络通讯
- 文件上传、下载核心功能改造，采用数据分包 + md5文件校验，确保文件传输准确性。
- NameNode + BackupNode的数据持久化机制优化，主要将JSON格式存储改造为protobuf序列化之后落地磁盘，可以提升序列化速度和优化占用磁盘空间大小。
- NameNode内存目录树改为Trie数据结构，提升读写文件目录树的效率。
- DataNode文件存储方式优化，文件存储改造根据文件名hash之后保存在65536个子文件夹中。
- DataNode上报文件存储信息改造为分批传输，避免保存文件过多，网络传输包过大。
- 基于BackupNode和NameNode实现主备切换功能，NameNode宕机后BackupNode可以顶上，实现高可用架构
- 实现多NameNode集群机制，解决超大规模文件存储带来的内存压力，实现元数据分片存储。实现了NameNode Controller选举、NameNode节点自动扩容、NameNode节点相互自动转发请求等功能。
- 实现了基于用户的多租户机制，上传、下载文件需要经过用户认证流程。用户可以通过配置DataNode集群实现不同用户之间的文件物理隔离机制。
- 基于Netty实现了HTTP协议，文件可以通过HTTP协议进行下载。
- 文件删除 + 垃圾箱功能。文件删除后移动到垃圾箱，可以从垃圾箱恢复文件
- 上传文件可以指定副本因子和自定义属性。
- 实现了WebUI界面功能，可以对用户进行CRUD操作，浏览用户文件目录树，删除和恢复用户文件/文件夹
- 实现了基本的运维功能，包括Maven打包、各节点的启动、停止脚本、命令行工具、和监控接入等



## 编译&运行


由于源码使用了protobuf作为序列化框架，所以下载代码之后需要执行以下命令，生成protobuf序列化文件


```
cd ruyuan-dfs/ruyuan-dfs-common
mvn protobuf:compile && mvn install
```


>  
>  

> 温馨提示：如果你的电脑是Apple M1芯片的，Protobuf编译可能会报错，这个问题可以通过配置指定使用x86架构解决，具体方式如下：



- 方式一：在ruyuan-dfs-common的pom.xml 中添加如下代码



```
<properties>
  <os.detected.classifier>osx-x86_64</os.detected.classifier>
</properties>
```


- 方式二：全局配置Maven，不用修改代码，在你的Maven的settings.xml(通常在~/.m2/settings.xml)文件下添加如下代码



```
<profile>
  <id>apple-silicon</id>
  <properties>
    <os.detected.classifier>osx-x86_64</os.detected.classifier>
  </properties>
</profile>

<activeProfiles>
  <activeProfile>default</activeProfile>
  <activeProfile>apple-silicon</activeProfile>
  ...你其他的profile
 </activeProfiles>
```


## 启动NameNode


打开配置NameNode的配置文件，在项目根目录下conf目录存在一个namenode.properties文件，打开此文件，修改以下内容：


```
base.dir=/srv/ruyuan-dfs/namenode  # 修改为你本机的一个路径
```


启动类为ruyuan-dfs-namenode模块下的类：com.ruyuan.dfs.namenode�.NameNode。我们可以运行他的main方法，
但是通常第一次运行是不成功的，会提示异常。


我们需要对启动程序进行一些配置，点击IDEA右上角运行按钮左边的下拉框。 选择 Edit Configurations...，在弹出框中，我们需要配置几个参数：


![](img/step1.png#id=Ccndh&originalType=binary&ratio=1&status=done&style=none)
主要看下面两个红框，需要配置一个JVM参数-Dlogback.configurationFile=conf/logback-namenode.xml 用于指定Logback的配置文件，
接着添加一个Program arguments为 conf/namenode.properties 用于指定NameNode的配置文件，接着就可以运行起来了。


## 启动BackupNode


BackupNode机器已经和NameNode集成在同一个module中了，启动类为com.ruyuan.dfs.backup.BackupNode
同样的，BackupNode也需要修改配置文件和启动参数：


![](img/step2.png#id=VPTOh&originalType=binary&ratio=1&status=done&style=none)
同样需要修改base.dir属性为你本机的一个路径，其他属性不变即可。启动参数配置如下：


![](img/step3.png#id=jPvcR&originalType=binary&ratio=1&status=done&style=none)


## 启动DataNode


修改conf/datanode.properties文件中的base.dir参数值为你本机电脑的一个路径


![](img/step4.png#id=Jrxgl&originalType=binary&ratio=1&status=done&style=none)


>  
>  

> 另外需要注意的是，如果你要启动多个DataNode节点，需要改为配置文件的值，其中datanode.id需要改成不同的数值，每个节点不一样，
base.dir需要改为不同的文件夹， 避免文件存储冲突，datanode.http.server和datanode.transpot.server的端口都需要改成不同的，避免端口冲突，
主机名也需要换成不同的，不然会造成DataNode注册混乱。因为NameNode是通过hostname来标识一个DataNode节点的。可以通过配置hosts文件



```
127.0.0.1 datanode01
127.0.0.1 datanode02
127.0.0.1 datanode03
```


配置启动参数：


![](img/step5.png#id=Gqbhp&originalType=binary&ratio=1&status=done&style=none)


## 运行客户端单元测试


如果上面几个节点都启动了，则可以开始进行单元测试看看效果了，但是在进行单元测试之前，需要先创建一个用户。


运行以下命令创建用户：


```
curl -H "Content-Type: application/json" -X POST -d '{"username": "admin","secret": "admin"}' "http://localhost:8081/api/user"
```


### 运行单元测试


接着就可以运行单元测试，打开ruyuan-dfs-client模块的test文件夹，查看测试类： com.ruyuan.dfs.client .FileSystemTest，直接执行：


![](img/step6.png#id=jgs9d&originalType=binary&ratio=1&status=done&style=none)


通过这个按钮则会将所有流程都测试一遍，包括上传文件、下载文件、创建文件夹等场景。
