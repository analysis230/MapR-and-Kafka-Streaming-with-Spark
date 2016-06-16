# MapR-and-Kafka-Streaming-with-Spark
Has programs that demonstrate consuming from MapR streams or Kafka Streams using Spark

SparkConsumer_kafkastreams.java:

Is a programs that reads from kafka streams and writes back into kafka streams.

You would need to set up zookeeper and brokers for it to be able to run. For any clarification drop in a mail at vipul.s.paul@gmail.com


SparkConsumer.java:

Reads from a MapR streams but cannot write back into it because currently the Java API does not allow to do it. Another repository
with scala code that allows you to do that would soon be added.

Again you would have to create the topics and edit them in the code yourself.

For clarification: drop and email at vipul.s.paul@gmail.com
