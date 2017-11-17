package com.zhiyou.kafkaClient.homwork_20171113;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileProducer {

	private KafkaProducer<String, byte[]> producer;
	private Properties properties;
	private FileInputStream fileInputStream;
	private byte[] byteContext = new byte[1024];
	private int len;
	
	private String fileName = "";
	private String topicStr = "";
	
	public FileProducer(){
		properties = new Properties();
		properties.put(
				"bootstrap.servers"
				, "master:9092k,master:9093");
		properties.put(
				"key.serializer"
				, "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(
				"value.serializer"
				, "org.apache.kafka.common.serialization.StringSerializer");
		
		producer = new KafkaProducer<String, byte[]>(properties);
	}
	
	
	public void sendRecorder(String fileName) throws Exception{
		
		fileInputStream = new FileInputStream(new File(fileName));
		int len = 0;
		while((len = fileInputStream.read(byteContext)) != -1){
			ProducerRecord<String, byte[]> record = 
					new ProducerRecord<String, byte[]>(topicStr, byteContext);
			producer.send(record);
		}
	}
	
	
	public void close(){
		
		producer.close();
	}

	
	public static void main(String[] args) throws Exception{
		FileProducer fileProducer = new FileProducer();
		fileProducer.sendRecorder(fileProducer.fileName);
		fileProducer.close();
	}
	
	
}
