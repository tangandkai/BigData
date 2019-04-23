package com.tang.spark.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumer extends Thread{

    Properties props = new Properties();
    private String topic;

    private Consumer<String,String> consumer;
    public KafkaConsumer(String topic){
        this.topic = topic;

        props.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        props.put("group.id", KafkaProperties.GROUP_ID);
        props.put("enable.auto.commit","true");
        props.put("auto.commit.interval.ms","1000");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(props);
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singleton(topic));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(10);
            for (ConsumerRecord<String,String> record:records){
                System.out.println("offset ="+record.offset()+"\t"+"key="+record.key()+"\t"+"value="+record.value());
                try {
                    Thread.sleep(4000);
                } catch (InterruptedException e) {
                    consumer.close();
                    e.printStackTrace();
                }
            }
        }
    }
}
