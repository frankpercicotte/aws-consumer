package com.group6.awsconsumer.services;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerService {
    public static void readMessage(String groupId) throws InterruptedException, ExecutionException{
        var consumer = new KafkaConsumer<String, String>(properties(groupId));
        consumer.subscribe(Collections.singletonList(System.getenv("KAFKA_TOPIC")));
        Boolean flagContinue = true;

        while (flagContinue) {
            var records = consumer.poll(Duration.ofMillis(1000));            
          
            for (ConsumerRecord<String, String> registro : records) {
                System.out.println("Teste ouvindo msg kafka:");
            	System.out.println("key: " + registro.key());
                System.out.println("value: " + registro.value());
                System.out.println("timestamp: " + registro.timestamp());
                if(registro.value().equals("exit")){                    
                    flagContinue = false;
                }
            }
        }
        System.out.println("Mensagem  finalização recebida.. tchau!!!");
    }

    private static Properties properties(String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_HOST"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); 
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }
}