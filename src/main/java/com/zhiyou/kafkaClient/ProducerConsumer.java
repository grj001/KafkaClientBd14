package com.zhiyou.kafkaClient;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ProducerConsumer {

	private KafkaConsumer<String, String> consumer;
	private Properties properties;
	
	public ProducerConsumer() {
		properties = new Properties();
		properties.put(
				"bootstrap.servers"
				, "master:9092,master:9093");
		properties.put(
				"key.deserializer"
				, "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put(
				"value.deserializer"
				, "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put(
				"group.id"
				, "java-group");
		
		consumer = new KafkaConsumer<String, String>(properties);
	}
	
	//订阅
	public void subscribeTopic(){
		ArrayList<String> topics = new ArrayList<String>();
		topics.add("from-java");
		consumer.subscribe(topics);
		//始终拉取
		while(true){
			//从kafka中拉取数据
			ConsumerRecords<String,String> records = consumer.poll(1000);
			for(ConsumerRecord<String,String> record : records){
				System.out.println("接收到的消息: partition"+record.partition());
				System.out.println("接收到的消息: offset"+record.offset());
				System.out.println("接收到的消息: key"+record.key());
				System.out.println("接收到的消息: value"+record.value());
			}
		}
		
	}
	
	
	
	
	
	
	
	public static void main(String[] args) {
		ProducerConsumer producerConsumer = new ProducerConsumer();
		producerConsumer.subscribeTopic();
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
