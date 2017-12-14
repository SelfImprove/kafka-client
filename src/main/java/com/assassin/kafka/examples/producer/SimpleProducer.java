package com.assassin.kafka.examples.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * kafak生产端简单示例
 * @author assassin
 * @date 2017年12月14日
 */
public class SimpleProducer {

	public static void main(String[] args) {
		 Properties props = new Properties();
		 props.put("bootstrap.servers", "mq2:9092");
		 props.put("acks", "all");
		 props.put("retries", 0);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 // 注册自定义分区
		 props.put("partitioner.class", "com.assassin.kafka.examples.partitioner.SimplePartitioner");
		 
		 Producer<String, String> producer = new KafkaProducer<>(props);
		 for (int i = 0; i < 100; i++)
		     producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)));

		 producer.close();
	}
	
}
