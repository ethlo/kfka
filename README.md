# kfka

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.ethlo.kfka/kfka/badge.svg)](https://maven-badges.herokuapp.com/maven-central/cz.jirutka.rsql/rsql-parser)
[![Build Status](https://travis-ci.org/ethlo/kfka.png?branch=master)](https://travis-ci.org/ethlo/kfka)
[![Coverage Status](https://coveralls.io/repos/github/ethlo/kfka/badge.svg?branch=master)](https://coveralls.io/github/ethlo/kfka?branch=master)
[![Hex.pm](https://img.shields.io/hexpm/l/plug.svg)](LICENSE)

This project aims to give a (small) subset of the benefits of [Apache Kafka](https://kafka.apache.org/) (persistent, queryable event queue) without the overhead of managing a Kafka cluster, which sometimes is overkill.

kfka uses a combination of a Hazelcast map which is backed by a persistent store. 
The durability guarantee is easily tweaked by the write policy of the map.

# Usage
```java
kfkaManager.addListener((msg)->
	 {
         //TODO: Handle message
     },
     new KfkaPredicate()
    .topic("bar")
    .offset(-100)
    .addPropertyMatch("userId", 123);
```
