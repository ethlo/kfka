# kfka

[![Maven Central](https://img.shields.io/maven-central/v/com.ethlo.kfka/kfka.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.ethlo.kfka%22%20a%3A%22kfka%22)
[![Build Status](https://travis-ci.org/ethlo/kfka.svg?branch=master)](https://travis-ci.org/ethlo/kfka)
[![Coverage Status](https://coveralls.io/repos/github/ethlo/kfka/badge.svg?branch=master)](https://coveralls.io/github/ethlo/kfka?branch=master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/ee97abb9994d44c7b61e533454368dd0)](https://www.codacy.com/app/ethlo/kfka?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=ethlo/kfka&amp;utm_campaign=Badge_Grade)
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
