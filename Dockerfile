# ============================================================
# Stage 1: Build NAR with Maven (Java 21)
# ============================================================
FROM maven:3.9.15-eclipse-temurin-21 AS builder

WORKDIR /build

# Optional: Cache dependencies separately
COPY pom.xml .
COPY nifi-opcua-service-api/pom.xml nifi-opcua-service-api/pom.xml
COPY nifi-opcua-service/pom.xml nifi-opcua-service/pom.xml
COPY nifi-opcua-processors/pom.xml nifi-opcua-processors/pom.xml
COPY nifi-opcua-nar/pom.xml nifi-opcua-nar/pom.xml

RUN mvn -B -q dependency:go-offline -DskipTests

# Copy full source
COPY . .

# Build NAR
RUN mvn -B clean install -DskipTests


# ============================================================
# Stage 2: Runtime Image – Apache NiFi 2.2.8
# ============================================================
FROM apache/nifi:2.2.8

# Copy custom NAR into NiFi lib directory
COPY --from=builder \
  /build/nifi-opcua-nar/target/*.nar \
  /opt/nifi/nifi-current/lib/

# Optional metadata
LABEL org.opencontainers.image.title="NiFi OPC-UA Bundle (Mondi Group)"
LABEL org.opencontainers.image.version="2.2.8"
LABEL org.opencontainers.image.vendor="Mondi Group"

# NiFi ports (nur Doku)
EXPOSE 8443 8080