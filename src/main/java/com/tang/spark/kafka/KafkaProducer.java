package com.tang.spark.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class KafkaProducer extends Thread{

    private String topic;
    Properties props = new Properties();

    private Producer<String,String> producer;
    public KafkaProducer(String topic){
        this.topic = topic;
        props.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
    }

    @Override
    public void run() {
        int message_id = 0;
        while (true){
            producer.send(new ProducerRecord<String, String>(topic, "the message_id=" + message_id++), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println(recordMetadata.serializedKeySize());
                }
            });
            System.out.println("the message_id="+message_id);
            try {
                Thread.sleep(4000);
//                producer.flush();
//                producer.commitTransaction();
            }catch (Exception e){
                producer.close();
                e.printStackTrace();
            }
        }
    }
}
