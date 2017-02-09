# kfka

This project aims to give a (small) subset of the benefits of [Apache Kafka](https://kafka.apache.org/) (persistent, queryable event queue) without the overhead of managing a Kafka cluster, which sometimes is overkill.

kfka uses a combination of a Hazelcast map which is backed by a persistent store. 
The durability guarantee is easily tweaked by the write policy of the map.

# Build status

[![Build Status](https://travis-ci.org/ethlo/kfka.png?branch=master)](https://travis-ci.org/ethlo/kfka)

# Maven repository
http://ethlo.com/maven

# Maven artifact
```xml
<dependency>
  <groupId>com.ethlo.kfka</groupId>
  <artifactId>kfka</artifactId>
  <version>${kfka.version}</version>
</dependency>
```

# Usage
```java
new KfkaPredicate(kfkaManager)
    .topic("bar")
    .offset(-100)
    .propertyMatch(Collections.singletonMap("userId", 123))
    .addListener((msg)->
     {
         //TODO: Handle message
     });
```
