package com.girija.git;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
       //create Producer Properties

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);


        for(int i = 0 ; i< 10 ;i++) {
            String topic = "second_topic";
            String key = "id " + Integer.toString(i);
            String value = "hello world " + Integer.toString(i);

            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key,value);
            logger.info("Key: "+ key);
            //Send Data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordmetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received Metadata \n" +
                                "Topic: " + recordmetadata.topic() + "\n" +
                                "Partition :" + recordmetadata.partition() + "\n" +
                                "Offset :" + recordmetadata.offset() + "\n" +
                                "TimeStamp: " + (recordmetadata.timestamp()));
                    } else {
                        logger.error("Error While producing ", e);

                    }
                }
            }).get();
        }

        //flush data
        producer.flush();
        //flush and close producer
        producer.close();

    }
}
