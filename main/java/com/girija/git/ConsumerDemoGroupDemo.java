package com.girija.git;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroupDemo {
    public static void main(String[] args){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroupDemo.class);

        String bootstrapservers = "localhost:9092";
        String groupID  = "fourth_group";
        String topic = "second_topic";
        //create Consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapservers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);

        //Create Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);


        //Subscribe to Topic
        consumer.subscribe(Arrays.asList(topic));

        //Consume the topic messageg
        while(true){
         ConsumerRecords<String,String> records =     consumer.poll(Duration.ofMillis(100));
         for(ConsumerRecord<String,String> record: records){
             logger.info("Key : "+ record.key() +"\n"+ "value: "+ record.value());
             logger.info("partition: "+ record.partition() + "\n"+ "offset: " + record.offset());
         }
        }


    }
}
