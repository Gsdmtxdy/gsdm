package com.imti.toES;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaOrderConsumer {
	
	private static KafkaConsumer<String, String> consumer = null;
	private static Properties props = null;
	
	static void initKafkaConsumer() {
		
		 props = new Properties();
	     props.setProperty("bootstrap.servers", "localhost:9092");
	     props.setProperty("group.id", "imti3");
	     props.setProperty("auto.offset.reset", "earliest");
	     props.setProperty("enable.auto.commit", "true");
	     props.setProperty("auto.commit.interval.ms", "1000");
	     props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     consumer = new KafkaConsumer<String,String>(props);
		
	}
	
	public static void consuemrOrder() {
		initKafkaConsumer();
		consumer.subscribe(Arrays.asList("order"));
		String order = null;
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> consumerRecord : records) {
				order = consumerRecord.value();
				ESUtiles.inserOrder("/orders/_doc", order.replaceAll("\\\\", "").replaceAll("\"\\{", "{").replaceAll("\\}\"", "}")
						.replaceAll("\"\\[", "[").replaceAll("\\]\"", "]"));
			}
			
		}
		
	}
	
	public static void consuemThreadOrder() {
		initKafkaConsumer();
		consumer.subscribe(Arrays.asList("order"));
		String order = null;
		ExecutorService executorService = new ThreadPoolExecutor(2, 10, 60, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
		String result = null;
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> consumerRecord : records) {
				order = consumerRecord.value();
				result = order.replaceAll("\\\\", "").replaceAll("\"\\{", "{").replaceAll("\\}\"", "}")
						.replaceAll("\"\\[", "[").replaceAll("\\]\"", "]");
				executorService.execute(new OrderThreadToES(result,"/order-thread/_doc",ESUtiles.restClint));
			}
			
			
		}
		
		
	}
	
	
	
	

	public static void main(String[] args) {

		consuemThreadOrder();
	}

}
