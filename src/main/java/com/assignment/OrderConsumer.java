package com.assignment;

import com.assignment.avro.Order;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {
    private static final String TOPIC = "orders";
    private static final String DLQ_TOPIC = "orders-dlq";

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

        Properties dlqProps = new Properties();
        dlqProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl != null ? kafkaUrl : "localhost:9092");
        dlqProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl != null ? schemaUrl : "http://localhost:8081");
        dlqProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        dlqProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        KafkaProducer<String, Order> dlqProducer = new KafkaProducer<>(dlqProps);

        consumer.subscribe(Collections.singletonList(TOPIC));

        double totalPrice = 0.0;
        long count = 0;

        System.out.println("Consumer started. Processing orders...");

        while (true) {
            ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(1000));

            for (var record : records) {
                Order order = record.value();
                boolean processed = false;
                int retryCount = 0;
                final int maxRetries = 3;

                while (!processed && retryCount < maxRetries) {
                    try {
                        if ("FailItem".equals(order.getProduct().toString())) {
                            throw new RuntimeException("Simulated failure for FailItem");
                        }

                        // Real-time aggregation
                        totalPrice += order.getPrice();
                        count++;
                        double runningAvg = totalPrice / count;

                        System.out.printf("Processed orderId=%s product=%s price=%.2f | Total Orders: %d | Running Avg: %.2f%n",
                                order.getOrderId(), order.getProduct(), order.getPrice(), count, runningAvg);

                        processed = true;

                    } catch (Exception e) {
                        retryCount++;
                        if (retryCount < maxRetries) {
                            System.out.printf("Processing failed for orderId=%s... Retrying %d/%d in 2 seconds%n",
                                    order.getOrderId(), retryCount, maxRetries);
                            Thread.sleep(2000); // Delay before retry
                        } else {
                            System.out.println("Error processing record after retries, sending to DLQ: " + e.getMessage());
                            ProducerRecord<String, Order> dlqRecord = new ProducerRecord<>(DLQ_TOPIC, record.key(), record.value());
                            dlqProducer.send(dlqRecord);
                            processed = true; // Mark as processed to avoid re-processing
                        }
                    }
                }
            }

            consumer.commitSync();
        }
    }
}