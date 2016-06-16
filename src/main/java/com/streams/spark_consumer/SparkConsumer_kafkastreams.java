package com.streams.spark_consumer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
//import org.apache.spark.examples.streaming.JavaDStreamKafkaWriter;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

import scala.Tuple2;
//import org.apache.spark.examples.streaming.StreamingExamples;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import kafka.serializer.StringDecoder;

import org.cloudera.spark.streaming.kafka.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.util.Properties;


public class SparkConsumer_kafkastreams {
    
	public static void main (String args[]) throws Exception
	{
		if (args.length < 2) {
		      System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
		          "  <brokers> is a list of one or more Kafka brokers\n" +
		          "  <topics> is a list of one or more kafka topics to consume from\n\n");
		      System.exit(1);
		    }
		
		class ProcessingFunc implements Function<String, KeyedMessage<String, String>> {
		    public KeyedMessage<String, String> call(String in) throws Exception {
		      return new KeyedMessage<String, String>("original", "null",in);
		    }
			}

		    //StreamingExamples.setStreamingLogLevels();
		
		  Properties producerConf = new Properties();
	      producerConf.put("serializer.class", "kafka.serializer.StringEncoder");
	      producerConf.put("key.serializer.class", "kafka.serializer.StringEncoder");
	      producerConf.put("metadata.broker.list", "localhost:9092");
	      producerConf.put("request.required.acks", "1");

		    String brokers = args[0];
		    String topics = args[1];

		    // Create context with a 2 seconds batch interval
		    SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JavaDirectKafkaWordCount");
		    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

		    Set<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		    Map<String, String> kafkaParams = new HashMap<String,String>();
		    kafkaParams.put("zookeeper.connect", "localhost:2181");
		    kafkaParams.put("metadata.broker.list", brokers);
		
		    
		    // Create direct kafka stream with brokers and topics
		    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
		        jssc,
		        String.class,
		        String.class,
		        StringDecoder.class,
		        StringDecoder.class,
		        kafkaParams,
		        topicsSet
		    );
		    
		    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
		        //@Override
		        public String call(Tuple2<String, String> tuple2) {
		          return tuple2._2();
		        }
		      });
		    
		    //int pass;

		    JavaDStream<String> words = lines.map(new Function<String, String>() {
		        //@Override
		        public String call(String x) {
		        			      	
		      	final String tempString = x;
		      	String[] tempArray = x.split(",");
		      	
		      	if (Double.parseDouble(tempArray[3]) == 0)
		      	{
		      		//fail_writer.write(tempString);
		      		System.out.println("Fail " + tempString);
		      	
		      	}
		      	
		      	else
		      	{
		      		//pass_writer.write(tempString);
		      		System.out.println("Pass " + tempString);
		      	}
		          return tempArray[3];
		        }
		      });
		      /*JavaDStream<String> Conditional = words.map(new Function<String,String>() {
		          @Override
		          public String call(String s) {
		          
		          	System.out.println("S is " + s);
		            return s; If I write something with the VI editore it 
		          }
		        });*/
		      
		      //messages.print();
		      //lines.print();
		      words.print();
		      //wordCounts.print();
		      
		      System.out.println("Execution is fine");
		      JavaDStreamKafkaWriter<String> writer = JavaDStreamKafkaWriterFactory.fromJavaDStream(lines);
		      
		      writer.writeToKafka(producerConf, new ProcessingFunc());
		      // Start the computation
		     //System.out.println(messages);
		      jssc.start();
		      jssc.awaitTermination();
	}
	
	
}
