package com.assignment;

import com.assignment.avro.Order;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {
    private static final String TOPIC = "orders";

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();

        String kafkaUrl = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        String schemaUrl = System.getenv("SCHEMA_REGISTRY_URL");

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl != null ? kafkaUrl : "localhost:9092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl != null ? schemaUrl : "http://localhost:8081");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));

        double totalPrice = 0.0;
        long count = 0;

        while (true) {
            ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(1000));

            for (var record : records) {
                Order order = record.value();

                // Real-time aggregation
                totalPrice += order.getPrice();
                count++;
                double runningAvg = totalPrice / count;

                System.out.printf("Processed orderId=%s product=%s price=%.2f | Total Orders: %d | Running Avg: %.2f%n",
                        order.getOrderId(), order.getProduct(), order.getPrice(), count, runningAvg);
            }

            consumer.commitSync();
        }
    }
}