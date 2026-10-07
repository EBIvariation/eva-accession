# Build stage
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

# Copy the full project
COPY . .
RUN mvn dependency:go-offline -B

# Build only eva-accession-ws and its required dependencies
RUN mvn -pl eva-accession-ws -am clean package -DskipTests -Dmaven.gitcommitid.skip=true

# Runtime stage
FROM eclipse-temurin:21-jre
WORKDIR /app

# Install curl for health checks
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd -r eva_submission_ws && useradd -r -g eva_submission_ws eva_submission_ws

# Create config directory for runtime application.properties mount
RUN mkdir -p /app/config /tmp/tomcat/logs && chown -R eva_submission_ws:eva_submission_ws /app /tmp/tomcat

# Copy the JAR file from build stage
COPY --from=build /app/eva-accession-ws/target/*.jar app.jar

# Set ownership
RUN chown -R eva_submission_ws:eva_submission_ws /app

USER eva_submission_ws

EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/eva/webservices/identifiers/readyz || exit 1

# JVM options for containerized environments
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:+UseG1GC --add-opens java.base/java.time=ALL-UNNAMED"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]