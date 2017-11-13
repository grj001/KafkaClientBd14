package com.zhiyou.kafkaClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class MenulCommitConsumer {

	
	private KafkaConsumer<String, String> consumer;
	private Properties properties;
	
	public MenulCommitConsumer() {
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
		properties.put(
				"enable.auto.commit"
				, "false");
		properties.put(
				"auto.offset.reset"
				, "earliest");
		
		consumer = new KafkaConsumer<String, String>(properties);
	}
	
	public void subscribeTopic(){
		List<String> topics = new ArrayList<String>();
		topics.add("from-java");
//		//指定分区
//		List<TopicPartition> topicPartitions = 
//				new ArrayList<>();
//		topicPartitions.add(new TopicPartition("from-java",0));
//		consumer.assign(topicPartitions);
		consumer.subscribe(topics);
		//始终拉取
		while(true){
			//从kafka中拉取数据
			ConsumerRecords<String,String> records = consumer.poll(1000);
			for(ConsumerRecord<String,String> record : records){
				System.out.println("接收到的消息partition: "+record.partition()+"\t"
									+"offset: "+record.offset()+"\t"
									+"key: "+record.key()+"\t"
									+"value: "+record.value());
			}
//			consumer.commitSync();
		}
	}
	
	
	//
	public void getOffsets(){
		OffsetAndMetadata offsets = 
				consumer.committed(
						new TopicPartition("from-java", 0));
		System.out.println(offsets+" : "+offsets.offset());
	}
	
	
	//消费这对topic的消费指定有两种方式, 1.consumer.subscribe(topic)
	//									2.consumer.assign(topicPartitions);
	public void consumerAssigned(){
		List<String> topics = 
				new ArrayList<>();
		topics.add("from-java");
		//指定分区
		List<TopicPartition> topicPartitions = 
				new ArrayList<>();
		topicPartitions.add(new TopicPartition("from-java",0));
		consumer.assign(topicPartitions);
		consumer.seek(new TopicPartition("from-java",0), 50);
		while(true){
			//从kafka中拉取数据
			ConsumerRecords<String,String> records = consumer.poll(1000);
			for(ConsumerRecord<String,String> record : records){
				System.out.println("接收到的消息: partition: "+record.partition());
				System.out.println("接收到的消息: offset: "+record.offset());
				System.out.println("接收到的消息: key: "+record.key());
				System.out.println("接收到的消息: value: "+record.value());
			}
			
//			System.out.println("提交");
//			consumer.commitSync();
		}
	}
	
	
	public void setCommitOffset(){
		Map<TopicPartition, OffsetAndMetadata> offsets = 
				new HashMap<>();
		offsets.put(new TopicPartition("from-java",0), new OffsetAndMetadata(20));
		List<String> topics = new ArrayList<>();
		topics.add("from-java");
		consumer.subscribe(topics);
		//指定位置提交某个分区的offset的值, 这个会在下一次
		consumer.commitSync(offsets);
		
		while(true){
			ConsumerRecords<String, String> records =
					consumer.poll(1000);
			for(ConsumerRecord< String, String> record :records){
				if(record.partition() == 0){
					System.out.println("接收到的消息: partition: "+record.partition());
					System.out.println("接收到的消息: offset: "+record.offset());
					System.out.println("接收到的消息: key: "+record.key());
					System.out.println("接收到的消息: value: "+record.value());
				}
			}
		}
	}
	
	
	public void exactlyOnceConsumer(){
		properties.setProperty("enable.auto.commit", "false");
		//重设offset
//		consumer.commitSync(offsets);
	}
	
	
	
	
	
	
	
	public static void main(String[] args) {
		MenulCommitConsumer producerConsumer = new MenulCommitConsumer();
//		producerConsumer.getOffsets();
		producerConsumer.subscribeTopic();
//		producerConsumer.consumerAssigned();
//		producerConsumer.setCommitOffset();
	}
	
	
	
}
