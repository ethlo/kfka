# kfka

[![Maven Central](https://img.shields.io/maven-central/v/com.ethlo.kfka/kfka.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.ethlo.kfka%22%20a%3A%22kfka%22)
[![License](https://img.shields.io/github/license/ethlo/kfka)](LICENSE)

This project aims to give a subset of the benefits of [Apache Kafka](https://kafka.apache.org/) (persistent, queryable
event queue) without the overhead of managing a Kafka cluster, which sometimes may be overkill for lower message
volumes.

Kfka stores messages in a database table through plain JDBC, and has no dependencies besides SLF4J, which makes it
ideal as an embedded message queue.

# Usage

```xml
<dependency>
    <groupId>com.ethlo.kfka</groupId>
    <artifactId>kfka-jdbc</artifactId>
    <version>${released.version}</version>
</dependency>
```

The examples below are the tests in [`kfka-jdbc/src/test/java/samples`](kfka-jdbc/src/test/java/samples), copied into
this file by the build. They use
[`CustomKfkaMessage`](kfka-jdbc/src/test/java/com/acme/CustomKfkaMessage.java), a message type with a queryable
`userId` property, and `send(...)` publishes a text message like the `publish` sample does.

### Table definition

```sql
CREATE TABLE `kfka`
(
    `message_id` varbinary(36) NOT NULL,
    `timestamp`  bigint        NOT NULL,
    `payload`    blob          NOT NULL,
    `topic`      varchar(255)  NOT NULL,
    `type`       varchar(255)  NOT NULL,
    `userId`     int,
    PRIMARY KEY (`message_id`)
);
```

Each queryable property of your message type, like `userId` here, needs a column of the same name.

### Configuration

${samples.setup}
### Listen for events

${samples.listening}
