akka-smr [![Build Status](https://travis-ci.org/msfrank/akka-smr.png?branch=master)](https://travis-ci.org/msfrank/akka-smr)
========

Akka-smr is a state-machine replication library built on top of the [Akka](http://akka.io) framework.
Akka-smr provides a set of fundamental operations which can be used to build higher-level distributed
abstractions, such as synchronization, naming, service discovery, and configuration management.  The
data model and operations are heavily influenced by [Apache Zookeeper](http://zookeeper.apache.org).
However, akka-smr differs from Zookeeper in one very significant way: it is a library, meant to be
integrated into an existing akka-cluster service.  Thus, a separate operational infrastructure for
Zookeeper is not needed, since the functionality is embedded directly in the service.

Requirements
------------

akka-smr depends on akka 2.3.0-RC2 and joda-time.
