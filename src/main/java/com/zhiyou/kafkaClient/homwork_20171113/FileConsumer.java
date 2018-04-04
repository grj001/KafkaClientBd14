package com.zhiyou.kafkaClient.homwork_20171113;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class FileConsumer {

	private KafkaConsumer<String, byte[]> consumer;
	private Properties properties;
	private String fileName = "";
	private String topic = "";
	
	public FileConsumer(){
		properties = new Properties();
		properties.put("bootstrap", "master:9092,master:9093");
		properties.put("group.id", "file_send");
		properties.put("auto.offset.reset", "earliest");
		properties.put(
				"key.deserializer"
				, "org.apazhe.kafka.common.serizlizer.StringDeserializer");
		properties.put(
				"value.deserializer"
				, "org.apache.kafka.common.serializer.StringDeserializer");
				
		consumer = new KafkaConsumer<String, byte[]>(properties);
	}
	
	public void subscribeTopic() throws IOException{
		FileOutputStream fileOutputStream =
				new FileOutputStream(new File(fileName));
		while(true){
			consumer.subscribe(Arrays.asList(topic));
			ConsumerRecords<String, byte[]> consumerRecords =
					consumer.poll(1000);
			for(ConsumerRecord<String, byte[]> consumerRecord : consumerRecords){
				fileOutputStream.write(consumerRecord.value());
			}
			fileOutputStream.close();
			return;
		}
	}
	
	
	public void close(){
		consumer.close();
	}
	
	
	public static void main(String[] args) throws IOException{
		FileConsumer fileConsumer = new FileConsumer();
		fileConsumer.subscribeTopic();
		fileConsumer.close();
	}
	
}
