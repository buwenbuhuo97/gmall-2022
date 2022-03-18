package com.buwenbuhuo.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

/**
 * Author 不温卜火
 * Create 2022-03-17 22:56
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: Kafka发送端测试代码
 */
public class MyKafkaSender {

    public static KafkaProducer<String, String> kafkaProducer=null;

    public static KafkaProducer<String, String> createKafkaProducer(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);
        }catch (Exception e){
            e.printStackTrace();
        }
        return producer;
    }

    public static  void send(String topic,String msg){
        if(kafkaProducer==null){
            kafkaProducer=createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String, String>(topic,msg));
    }
}

