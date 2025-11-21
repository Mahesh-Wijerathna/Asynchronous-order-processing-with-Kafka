# Stage 1: Build the application using Maven
FROM maven:3.8.5-openjdk-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: Create the runtime image
FROM eclipse-temurin:17-jdk
WORKDIR /app
COPY --from=build /app/target/kafka-assignment-1.0-SNAPSHOT.jar app.jar
# Entry point selected via docker-compose command
