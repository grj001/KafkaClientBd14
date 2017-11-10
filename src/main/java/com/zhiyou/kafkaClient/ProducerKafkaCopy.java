package com.zhiyou.kafkaClient;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerKafkaCopy {

	private KafkaProducer<String , String> producer;
	private Properties properties;
	
	public ProducerKafkaCopy() {
		properties = new Properties();
		properties.put(
				"bootstrap.servers"
				, "master:9092, master:9093");
		properties.put(
				"key.serializer"
				, "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(
				"value.serializer"
				, "org.apache.kafka.common.serialization.StringSerializer");
		
		producer = new KafkaProducer<String , String>(properties);
	}
	
	
	public void sendRecorder(String key, String value){
		
		ProducerRecord<String, String> record = 
				new ProducerRecord<String, String>("from-java-copy", key, value);
		producer.send(record);
	}
	
	public void close(){
		producer.flush();
		producer.close();
	}
	
	public static void main(String[] args) throws InterruptedException {
		ProducerKafkaCopy client =
				new ProducerKafkaCopy();
		for(int i=0;i<100;i++){
			client.sendRecorder("key"+i, "value"+i);
			System.out.println("key"+i+"value"+i);
			Thread.sleep(500);
		}
		client.close();
	}
	
}
