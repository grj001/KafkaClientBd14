package com.zhiyou.kafkaClient;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerKafka {

	private KafkaProducer<String, String> producer;
	private Properties properties;
	
	public ProducerKafka() {
		properties = new Properties();
		properties.put(
				"bootstrap.servers"
				, "master:9092,master:9093");
		properties.put(
				"key.serializer"
				, "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(
				"value.serializer"
				, "org.apache.kafka.common.serialization.StringSerializer");
		
		producer = new KafkaProducer<String, String>(properties);
	}
	
	public void sendRecorder(String key, String value){
		ProducerRecord<String, String> record = 
				new ProducerRecord<String, String>("from-java", key, value);
		producer.send(record);
	}
	
	
	
	public void assignPartitions(String key, String value){
		
		ProducerRecord<String, String> record = 
				new ProducerRecord<String, String>("from-java", 0, key, value);
		producer.send(record);
		
	}
	
	
	
	public void sendRecorderWithCallback(String key, String value){
		final Logger logger = LoggerFactory.getLogger(ProducerKafka.class);
		
		ProducerRecord<String, String> record = 
				new ProducerRecord<String, String>("from-java", key, value);
		
		producer.send(record,new Callback() {
			//回调方法
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				
				if(exception == null){

					logger.info("储存位置"+metadata.partition());
					logger.info("offset"+metadata.offset());
					logger.info("时间"
					+new SimpleDateFormat("HH:mm:ss")
					.format(
							new Date(
									metadata.timestamp())));
				}else{
					logger.warn("服务端出现异常: ");
					exception.printStackTrace();
				}
			}
		});
	}
	
	
	
	
	
	
	
	
	public void close(){
		producer.flush();
		producer.close();
	}
	

	
	public void getTopicPartitions(String topic){
		
		List<PartitionInfo> partitionsInfos = producer.partitionsFor(topic);
		System.out.println("*************");
		for(PartitionInfo partitionsInfo : partitionsInfos){
			System.out.println(partitionsInfo);
		}
	}
	
	
	public void getMetrics(){
		Map<MetricName, ? extends Metric> metrics = producer.metrics();
		System.out.println("*************");
		for(MetricName name:metrics.keySet()){
			System.out.println(name.name()+"----"+metrics.get(name).value());
		}
	}
	
	

	
	
	
	public static void main(String[] args) {
		ProducerKafka client = new ProducerKafka();
//		for(int i=0;i<100;i++){
//			client.sendRecorder("key"+i, "value"+i);
//		}
		client.getMetrics();
		client.getTopicPartitions("from-java");
		
//		for(int i=0;i<100;i++){
//			client.assignPartitions("key"+i, "value"+i);
//		}
		for(int i=0;i<100;i++){
			client.sendRecorderWithCallback("key"+i, "value"+i);
		}
		
		client.close();
	}
	
	
	
}
