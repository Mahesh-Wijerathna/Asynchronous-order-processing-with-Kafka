##
- **Name**: Wijerathna W.M.M.T
- **Registration Number**: EG/2020/4291
##

# Asynchronous Order Processing with Kafka

This project demonstrates an asynchronous order processing system using Apache Kafka, Zookeeper, Schema Registry, and Kafka UI. It includes a producer that generates orders, a consumer that processes them with retry logic and dead-letter queue (DLQ) handling, and a DLQ consumer for monitoring failed messages.



## Architecture

- **Zookeeper**: Manages Kafka cluster coordination.
- **Kafka Broker**: Handles message publishing and subscribing.
- **Schema Registry**: Manages Avro schemas for data serialization.
- **Kafka UI**: Web interface for monitoring Kafka topics and messages.
- **Producer**: Generates random orders and sends them to the "orders" topic.
- **Consumer**: Processes orders from the "orders" topic, performs aggregation, and handles failures by retrying or sending to DLQ.
- **DLQ Consumer**: Consumes failed messages from the "orders-dlq" topic for monitoring.

## Technologies Used

- Java 17
- Apache Kafka
- Apache Avro
- Confluent Schema Registry
- Docker & Docker Compose
- Maven

## Prerequisites

- Docker and Docker Compose installed on your system.

## Setup and Running

1. **Clone the repository**:
   ```
   git clone https://github.com/Mahesh-Wijerathna/Asynchronous-order-processing-with-Kafka.git
   cd Asynchronous-order-processing-with-Kafka
   ```

2. **Start the infrastructure**:
   ```
   docker-compose up -d zookeeper broker schema-registry kafka-ui
   ```
   This starts Zookeeper, Kafka broker, Schema Registry, and Kafka UI.

3. **Build and run the applications**:
   - **Producer**:
     ```
     docker-compose up --build producer
     ```
   - **Consumer**:
     ```
     docker-compose up --build consumer
     ```
   - **DLQ Consumer**:
     ```
     docker-compose up --build dlq-consumer
     ```

   Or run all at once:
   ```
   docker-compose up --build 
   ```

4. **View Logs in separate interfaces**
Navigate to docker compose file location and run the following commands to view logs for each application:
Open separate terminal windows and run:
```
docker compose logs producer-app --follow --tail=10
docker compose logs consumer-app --follow --tail=10
docker compose logs dlq-consumer-app --follow --tail=10
```

Open the same terminal and run:
```
docker compose logs producer-app consumer-app dlq-consumer-app --follow --tail=20
```

Open Docker Desktop application and go to containers/Apps section to view logs for each application.

5. **Access Kafka UI**:
   Open http://localhost:8082 in your browser to monitor topics, messages, and consumers.

## How It Works

- The **Producer** generates orders with random products and prices every 10 seconds. It uses Avro serialization and sends to the "orders" topic.
- The **Consumer** subscribes to "orders", processes each order, and computes a running average price. For "FailItem" products, it simulates failure, retries up to 3 times, and if still failing, sends the message to the "orders-dlq" topic.
- The **DLQ Consumer** listens to "orders-dlq" and logs failed orders for manual review.

## Configuration

- Kafka Bootstrap Servers: `broker:29092` (internal), `localhost:9092` (external)
- Schema Registry: `http://schema-registry:8081`
- Topics: `orders`, `orders-dlq`

## More Information

- **pom.xml**: Maven configuration file defining project metadata, dependencies (Kafka, Avro, Confluent libraries), and build plugins for compiling Java 17, generating Avro classes, and creating a fat JAR.
- **Dockerfile**: Multi-stage Docker build file using Maven to compile the application and Eclipse Temurin JDK 17 for runtime, producing a runnable JAR image.
- **Java Files**: 
  - OrderProducer.java: Generates and sends Avro-serialized orders to Kafka topic.
  - OrderConsumer.java: Consumes orders, processes with retry logic, aggregates prices, and sends failures to DLQ.
  - OrderDLQConsumer.java: Monitors and logs messages from the dead-letter queue.
- **order.avsc**: Avro schema file defining the Order record structure with fields: orderId (string), product (string), price (float).
- **Business Logics**: 
  - Producer: Random order generation with simulated products and prices, sent every 10 seconds.
  - Consumer: Real-time price aggregation, failure simulation for "FailItem", retry mechanism (up to 3 attempts), DLQ routing for persistent failures.
  - DLQ Consumer: Passive monitoring of failed orders for auditing and manual intervention.