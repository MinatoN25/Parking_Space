package com.parkingmanagement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@SpringBootApplication
public class GlParkingManagementApplication {

	public static void main(String[] args) {
		SpringApplication.run(GlParkingManagementApplication.class, args);
	}

	@Autowired
	private KafkaTemplate<String, String> template;

	@Bean
	public NewTopic topic() {
		return TopicBuilder.name("live_avl_free_space").partitions(6).replicas(1).build();
	}

	private int j;

	@Bean
	public ApplicationRunner sendMessage() {
		return args -> {
			for (;;) {
				template.send("live_avl_free_space", "Parking " + ++j, String.valueOf(Math.round(Math.random() * 100)));
				Thread.sleep(2000, 0);
			}
		};
	}

	@KafkaListener(topics = "live_avl_free_space")
	public void listenToTopics(ConsumerRecord<String, String> recordA) throws InterruptedException {
		System.out.println("Receiver Module");
		System.out.println("Key:- " + recordA.key());
		System.out.println("Value:- " + recordA.value());
		System.out.println("partition:- " + recordA.partition());
		System.out.println("timestamp:- " + recordA.timestamp());
		System.out.println("Topic:- " + recordA.topic());
		System.out.println("offset:- " + recordA.offset());
		System.out.println("--------------------------------------------");

		Map<String, Integer> parkingSpace = new HashMap<>();
		parkingSpace.put(recordA.key(), Integer.parseInt(recordA.value()));

		Thread.sleep(2000);
		parkingSpace.entrySet().stream().filter(s -> s.getValue() < 10)
				.forEach(s -> template.send("live_avbl_empty_space_warning", s.getKey(), String.valueOf(s.getValue())));
	}

	@Bean
	public NewTopic topic1() {
		return TopicBuilder.name("live_avbl_empty_space_warning").partitions(6).replicas(1).build();
	}

	private List<KTable> parking = new ArrayList<>();

	@KafkaListener(topics = "live_avbl_empty_space_warning")
	public void listenToAvlPark(ConsumerRecord<String, String> recordB) {
		try {
			System.out.println(recordB.key() + " Space: " + recordB.value());
			KTable parkingTable = new KTable();
			parkingTable.setName(recordB.key());
			parkingTable.setSpace(Integer.parseInt(recordB.value()));
			
			parking.add(parkingTable);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@GetMapping("/getFreeSpace")
	public ResponseEntity<?> getFreeSpace() {
		return new ResponseEntity<>(parking, HttpStatus.OK);
	}

}