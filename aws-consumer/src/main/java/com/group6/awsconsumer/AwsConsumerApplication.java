package com.group6.awsconsumer;

import java.util.concurrent.ExecutionException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

import com.group6.awsconsumer.services.KafkaConsumerService;


@EnableKafka
@SpringBootApplication
public class AwsConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(AwsConsumerApplication.class, args);
		System.out.println("Lendo mensagens ...");
        var groupId = System.getenv("KAFKA_GROUP_ID_READER");
        System.out.println(groupId);
        try {
			KafkaConsumerService.readMessage(groupId);
		} catch (InterruptedException | ExecutionException e) {
			System.out.println("Deu ruim veja log:\n");
			e.printStackTrace();
		}
	}

}
