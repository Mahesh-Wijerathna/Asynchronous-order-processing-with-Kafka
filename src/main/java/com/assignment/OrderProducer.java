package com.assignment;

import com.assignment.avro.Order;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

import java.util.Scanner;

public class OrderProducer {
    private static final String TOPIC = "orders";

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();

        String kafkaUrl = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        String schemaUrl = System.getenv("SCHEMA_REGISTRY_URL");

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl != null ? kafkaUrl : "localhost:9092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl != null ? schemaUrl : "http://localhost:8081");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
        int orderIdCounter = 1;
        Random random = new Random();
        
        String[] products = {"Laptop", "Mouse", "Keyboard", "Monitor", "Headphones", "FailItem"};
        
        System.out.println("Producer started. Automatically generating orders...");
        System.out.println("Note: 'FailItem' products will trigger failure simulation.");

        while (true) {
            try {
                // Select random product
                String product = products[random.nextInt(products.length)];
                
                // Generate random price between 10 and 1000
                float price = 10 + random.nextFloat() * 990;
                
                Order order = Order.newBuilder()
                        .setOrderId(String.valueOf(orderIdCounter++))
                        .setProduct(product)
                        .setPrice(price)
                        .build();

                ProducerRecord<String, Order> record = new ProducerRecord<>(TOPIC, order.getOrderId().toString(), order);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("Sent orderId=%s product=%s price=%.2f%n",
                                order.getOrderId(), order.getProduct(), order.getPrice());
                    } else {
                        exception.printStackTrace();
                    }
                });
                
                // Wait 10 seconds between messages
                Thread.sleep(10000);
                
            } catch (InterruptedException e) {
                System.out.println("Producer interrupted. Shutting down...");
                break;
            }
        }

        producer.close();
    }
}
