# kfka

[![Maven Central](https://img.shields.io/maven-central/v/com.ethlo.kfka/kfka.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.ethlo.kfka%22%20a%3A%22kfka%22)
[![Build Status](https://travis-ci.org/ethlo/kfka.svg?branch=master)](https://travis-ci.org/ethlo/kfka)
[![Coverage Status](https://coveralls.io/repos/github/ethlo/kfka/badge.svg?branch=master)](https://coveralls.io/github/ethlo/kfka?branch=master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/ee97abb9994d44c7b61e533454368dd0)](https://www.codacy.com/app/ethlo/kfka?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=ethlo/kfka&amp;utm_campaign=Badge_Grade)
[![Hex.pm](https://img.shields.io/hexpm/l/plug.svg)](LICENSE)

This project aims to give a subset of the benefits of [Apache Kafka](https://kafka.apache.org/) (persistent, queryable event queue) without the overhead of managing a Kafka cluster, which sometimes may be a bit overkill for lower message volumes.

Kfka currently supports MySQL as a back-end, and has zero dependencies which makes it ideal as an embedded message queue.

# Usage
```java
kfkaManager.addListener((msg)->
{
  // TODO: Handle message
},
new KfkaPredicate()
  .topic("my_topic")
  .rewind(100) // Rewind (up to) 100 messages
```

Use as a backend for SSE/Websockets messages
```java
kfkaManager.addListener((msg)->
{
  // TODO: Handle message
},
new KfkaPredicate()
  .topic("chat")
  .lastSeenMessageId(45_610) // Next message will be 45,611
  .addPropertyMatch("userId", 123); // Filtering on custom property
```
