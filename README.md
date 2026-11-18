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
    <version>2.0.1</version>
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

#### configuration <sup>[source](kfka-jdbc/src/test/java/samples/setup/SetupSamples.java#L52-L87)</sup>

Store messages in a database table, keep them for 3 days, and evict expired messages every 30 minutes.
Message ids must sort in the order the messages were added, so this generator is based on the clock and
strictly increasing.

```java
final Duration retentionTime = Duration.ofDays(3);
final Duration cleanInterval = Duration.ofMinutes(30);

final DataSource dataSource = dataSource();
final RowMapper<CustomKfkaMessage> rowMapper = rs -> new CustomKfkaMessageBuilder()
        .userId(rs.getInt("userId"))
        .payload(rs.getBytes("payload"))
        .timestamp(OffsetDateTime.ofInstant(Instant.ofEpochMilli(rs.getLong("timestamp")), ZoneOffset.UTC))
        .topic(rs.getString("topic"))
        .type(rs.getString("type"))
        .messageId(rs.getString("message_id"))
        .build();

final AtomicLong lastId = new AtomicLong();
final MessageIdGenerator idGenerator = () -> String.format("%020d",
        lastId.updateAndGet(previous -> Math.max(previous + 1, System.currentTimeMillis() * 1_000)));

final KfkaMessageStore<CustomKfkaMessage> msgStore =
        new JdbcKfkaMessageStore<>(dataSource, rowMapper, retentionTime);
final KfkaManager<CustomKfkaMessage> kfkaManager = new KfkaManagerImpl<>(msgStore, idGenerator);

final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
final long interval = cleanInterval.toMillis();
scheduler.scheduleAtFixedRate(kfkaManager::evictExpired, interval, interval, TimeUnit.MILLISECONDS);

assertThat(kfkaManager.size()).isZero();
scheduler.shutdownNow();
```

#### publish <sup>[source](kfka-jdbc/src/test/java/samples/setup/SetupSamples.java#L89-L106)</sup>

Publish a message. The manager assigns the message id, and the timestamp if it has none.

```java
final KfkaManager<CustomKfkaMessage> kfkaManager = kfkaManager();

final CustomKfkaMessage msg = kfkaManager.add(new CustomKfkaMessageBuilder()
        .userId(123)
        .payload("Hello".getBytes(StandardCharsets.UTF_8))
        .topic("chat")
        .type("text")
        .build());

assertThat(msg.getMessageId()).isNotNull();
assertThat(kfkaManager.size()).isEqualTo(1);
```


### Listen for events

#### rewind <sup>[source](kfka-jdbc/src/test/java/samples/listening/ListeningSamples.java#L38-L56)</sup>

Listen for messages on a topic. The listener first gets (up to) the last 100 stored messages that match, then
every new one. The return value is the number of stored messages it was sent.

```java
final KfkaManager<CustomKfkaMessage> kfkaManager = kfkaManager();
send(kfkaManager, "my_topic", 1);
send(kfkaManager, "other_topic", 1);
send(kfkaManager, "my_topic", 2);

final List<CustomKfkaMessage> received = new ArrayList<>();
final int replayed = kfkaManager.addListener(received::add, new KfkaPredicate().topic("my_topic"), 100);
assertThat(replayed).isEqualTo(2);

send(kfkaManager, "my_topic", 3);
assertThat(received).hasSize(3);
```

#### resumeAfterLastSeenMessage <sup>[source](kfka-jdbc/src/test/java/samples/listening/ListeningSamples.java#L58-L76)</sup>

Use as a back-end for Server-Sent Events or WebSockets. A client that reconnects gets everything after the last
message id it saw, here only the messages for one user, filtered on a custom property.

```java
final KfkaManager<CustomKfkaMessage> kfkaManager = kfkaManager();
final CustomKfkaMessage seen = send(kfkaManager, "chat", 123);
send(kfkaManager, "chat", 456);
final CustomKfkaMessage missed = send(kfkaManager, "chat", 123);

final String lastSeenMessageId = seen.getMessageId(); // For SSE, the Last-Event-ID header
final List<CustomKfkaMessage> received = new ArrayList<>();
final KfkaPredicate predicate = new KfkaPredicate().topic("chat").addPropertyMatch("userId", 123);
kfkaManager.addListener(received::add, predicate, lastSeenMessageId);

assertThat(received).containsExactly(missed);
```


