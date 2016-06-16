package com.streams.spark_consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;

import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;
import org.apache.spark.streaming.Durations;

import scala.Tuple2;


import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.Properties;

import kafka.producer.KeyedMessage;


/*//import kafka.javaapi.producer.Producer;
//import java.io.IOException;
//import kafka.serializer.StringDecoder;
//import java.util.Iterator;
//import kafka.producer.ProducerConfig;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.apache.spark.examples.streaming.JavaDStreamKafkaWriter;
//import org.cloudera.spark.streaming.kafka.*;
//import org.apache.spark.streaming.kafka.producer.*;
//import org.apache.spark.streaming.dstream.DStream;
//import org.apache.spark.examples.streaming.StreamingExamples;*/







public class SparkConsumer {
	
	private static final Pattern SPACE = Pattern.compile(",");
	

	static final String CHECK_STREAM_TOPICS = "/user/vipulrajan/streaming/original:sensor";
    static final String KAFKA_BROKER_STRING = "localhost:9092";
    static final String CONSUMER_GROUP_ID = "mapr";
    static final String KAFKA_OFFSET_RESET = "earliest";
    static final String KAFKA_POLL_TIMEOUT = "1000";
    
    public static Map<String, String> buildStreamPropertiesMap() {
        Map<String, String> streamPropertiesMap = new HashMap<String,String>();
        //streamPropertiesMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER_STRING);
        streamPropertiesMap.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        streamPropertiesMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        streamPropertiesMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        streamPropertiesMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KAFKA_OFFSET_RESET);
        streamPropertiesMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        streamPropertiesMap.put("spark.kafka.poll.time", KAFKA_POLL_TIMEOUT);
        return streamPropertiesMap;
    }

	public static void main (String args[]) throws Exception
	{
		
		class ProcessingFunc implements Function<String, KeyedMessage<String, String>> {
		    public KeyedMessage<String, String> call(String in) throws Exception {
		      return new KeyedMessage<String, String>("original", "null",in);
		    }
			}

		 
		    SparkConf sparkConf = new SparkConf().setMaster("local[2]")
	                .setAppName("MapRtoSpark")
	                .set("spark.streaming.backpressure.enabled", "true")
	                .set("spark.streaming.receiver.maxRate", Integer.toString(2000000))
	                .set("spark.streaming.kafka.maxRatePerPartition", Integer.toString(2000000));
		    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

		    Set<String> topicsSet = new HashSet<String>(Arrays.asList(CHECK_STREAM_TOPICS.split(",")));
		    Map<String, String> kafkaParams = buildStreamPropertiesMap();
		    
		    
	
		    
		    // Create direct kafka stream with brokers and topics
		    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
		        jssc,
		        String.class,
		        String.class, 
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
		    
		      
		      words.print();
		      
		      System.out.println("It's working");
		      jssc.start();
		      jssc.awaitTermination();
	}
	
	
}
