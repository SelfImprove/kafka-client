package com.assassin.kafka.examples.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * kafka消费端简单示例
 * @author assassin
 * @date 2017年12月14日
 */
public class SimpleConsumer {

	public static void main(String[] args) {
		 Properties props = new Properties();
	     props.put("bootstrap.servers", "mq2:9092");
	     props.put("group.id", "test");
	     props.put("enable.auto.commit", "true");
	     props.put("auto.commit.interval.ms", "1000");
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
	     consumer.subscribe(Arrays.asList("test"));
	     try {
	    	 while (true) {
	    		 ConsumerRecords<String, String> records = consumer.poll(100);
	    		 for (ConsumerRecord<String, String> record : records)
	    			 System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
	    	 }
	     } finally{
	    	 consumer.close();
	     }
	}
}
