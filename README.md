Scribengin
==========
Pronounced Scribe Engine  

Scribengin is a high-performance persistent event/loggin transport that registers data under defined schemas in a variety of end systems

Reads data from
- Kafka
- AWS Kinesis

Writes data to:
- HDFS, Hbase and Hive with HCat Integration

Additonal:
- Monitoring with Ganglia
- Heart Alerting with Nagios


This is part of [NeverwinterDP the Data Pipeline for Hadoop](https://github.com/DemandCube/NeverwinterDP)

Community
====
- [Mailing List](https://groups.google.com/forum/#!forum/scribengin)
- IRC channel #Scribengin on irc.freenode.net


The Problem
======
The core problem is how to reliably and at scale have a distributed application write data to multiple destination data systems.  This requires the ability to todo data mapping, partitioning with optional filtering to the destination system.


Potential Implementation Strategies
======

Poc
- Storm
- Spark-streaming
- Yarn
  - Local Mode (Single Node No Yarn)
  - Distributed Standalone Cluster (No-Yarn)
  - Hadoop Distributed (Yarn)

There is a question of how to implement quaranteed delivery of logs to end systems.  
- Storm to HCat
- Storm to HBase
- Create Framework to pick other destination sources

Architecture
======
![Scribengin Fully Distributed Mode in Yarn](diagrams/fully_distributed_yarn_v1.png?raw=true "A Highlevel Diagram of Fully Distributed in Yarn")
![Scribengin Fully Distributed Mode Standalone](diagrams/fully_distributed_standalone_v1.png?raw=true "A Highlevel Diagram of Fully Distributed")
![Scribengin Pseudo Distributed Mode](diagrams/pseudo_distributed_standalone_v1.png?raw=true "A Highlevel Diagram of Pseudo Distributed")
![Scribengin Standalone Mode](diagrams/standalone_v1.png?raw=true "A Highlevel Diagram of Standalone Mode")

Milestones
======
- [ ] Architecture Proposal
- [ ] Kafka -> HCatalog
- [ ] Notification API
- [ ] Notification API Close Partitions HCatalog
- [ ] Ganglia Integration
- [ ] Nagios Integration
- [ ] Unix Man page
- [ ] Guide
- [ ] Untar and Deploy - Work out of the box
- [ ] CentOS Package
- [ ] CentOS Repo Setup and Deploy of CentOS Package
- [ ] RHEL Package
- [ ] RHEL Repo Setup and Deploy of CentOS Package
- [ ] Scribengin/Ambari Deployment
- [ ] Scribengin/Ambari Monitoring/Ganglia
- [ ] Scribengin/Ambari Notification/Nagios


Contributors
=====
- [Steve Morin](https://github.com/smorin)

Related Project
----
- [Heka] (http://heka-docs.readthedocs.org/en/latest/)
- 

Research
----
- [Peterson's Algorithm](http://en.wikipedia.org/wiki/Peterson's_algorithm)
- [Google's Chubby](http://research.google.com/archive/chubby.html)
- [Distributed Lock Manager](http://en.wikipedia.org/wiki/Distributed_lock_manager)

Yarn Documentation
- [Slides about Yarn](http://www.slideshare.net/hortonworks/apache-hadoop-yarn-enabling-nex/)

Keep your fork updated
====
[Github Fork a Repo Help](https://help.github.com/articles/fork-a-repo)


- Add the remote, call it "upstream":

```
git remote add upstream git@github.com:DemandCube/Scribengin.git
```
- Fetch all the branches of that remote into remote-tracking branches,
- such as upstream/master:

```
git fetch upstream
```
- Make sure that you're on your master branch:

```
git checkout master
```
- Merge upstream changes to your master branch

```
git merge upstream/master
```

