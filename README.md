# kafka-flow
Kafka-flow is a kafka client that uses kotlin flow and coroutines to connect to kafka.

This library has two parts:
* A low level client that output kafka records in a flow stream.
* A higher level api that helps for common use cases when using kafka

Kafka-flow is under heavy development and api are still subject to major changes.  
It is already extensively tested in a private project (I'll migrate those test when I have time).  
The low level client is mostly stable.

# Installation

It is published on JitPack.
You can find instructions and latest version [here](https://jitpack.io/#Jeff-Gillot/kafka-flow)

```groovy
allprojects {
	repositories {
		maven { url 'https://jitpack.io' }
    }
}
```

```groovy
dependencies {
    implementation 'com.github.Jeff-Gillot.kafka-flow:kafka-flow-client:1.0.2'
}
```

# Usage
## Low level client

```kotlin
val consumer = KafkaFlowConsumerWithGroupIdImpl(
    properties(), 
    listOf(topic.name), 
    StartOffsetPolicy.earliest(), 
    AutoStopPolicy.never()
)

launch {
    consumer.startConsuming()
        .deserializeValue { String(it) }
        .values()
        .collect { record = it }
}
```

## Higher level api
The higher level api uses topic descriptors.  
Topic descriptor give a structured way to interact with a specific topic. 
See: [topic-descriptor.kt](kafka-flow-topic-descriptor/src/main/kotlin/kafka/flow/TopicDescriptor.kt)
It serves as a contract for a topic:
* Serde for value
* Serde for key
* Partitioning
* Timestamp extraction
This allows to create an extensible api on top of the low level consumer.  
  
Here is a simple example for a kafka to kafka stream:
```kotlin
launch {
    TestServer.from(testTopic1)
        .consumer()
        .withGroupId("GroupId")
        .autoOffsetResetEarliest()
        .consumeUntilStopped()
        .startConsuming()
        .ignoreTombstones()
        .mapValueToOutput { KafkaOutput.forValue(TestServer, testTopic2, it) }
        .writeOutputToKafkaAndCommit()
}
```

This also contains a thread safe and asynchronous transaction manager.  
It creates transaction for every message, and allow for out of order commit of individual messages.

## Testing
kafka-flow also provides an extensive testing library for kafka.  

```kotlin
val testTopic = TestTopicDescriptor.next()
val expected = TestObject.random()

TestServer.on(testTopic).send(expected)

TestServer.topicTester(testTopic).expectMessage {
    condition("same record") { expectThat(it).isEqualTo(expected) }
}
```
TestServer is a kafka server started using TestContainers.

## Extensibility
Thanks to extensions functions, this library is easy to extend with your own requirements.  
See [extensions](kafka-flow-client/src/main/kotlin/kafka/flow/consumer/ClientExtensions.kt) for more details.

## Full documentation
I am working on a full documentation.  
It takes time, I will update as soon as it's ready.

# Licence
Copyright (c) 2021 Jeff-Gillot

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.