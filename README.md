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


This is part of [NeverwinterDP](https://github.com/DemandCube/NeverwinterDP)

Community
====
- [Mailing List](https://groups.google.com/forum/#!forum/scribengin)
- IRC channel #Scribengin on irc.freenode.net


The Problem
======
The core problem is how to log data from a rest call and log it in high-performance way that 
allows for the delivery of messages to Kafka even when the connection is down.

Potential Implementation Strategies
======
There is a question of how to implement quaranteed delivery of logs to end systems.  
- Storm to HCat
- Storm to HBase
- Storm to Presto
- Create Framework to pick other destination sources


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
- [Juan Manuel Clavijo](https://github.com/PROM3TH3U5)

Related Project
----
- [Heka] (http://heka-docs.readthedocs.org/en/latest/)
