package com.girija.git;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Properties;

public class ProducerDemoWithCallBack {
    public static void main(String[] args) {
       //create Producer Properties

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);


        for(int i = 0 ; i< 10 ;i++) {

            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("second_topic", "hello-World"+ Integer.toString(i));
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
            });
        }

        //flush data
        producer.flush();
        //flush and close producer
        producer.close();

    }
}
