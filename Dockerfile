# One image per service: docker build --build-arg SERVICE=order-service .
ARG SERVICE=order-service

# --- Build -------------------------------------------------------------------
FROM eclipse-temurin:21-jdk-alpine AS build
ARG SERVICE
WORKDIR /workspace

COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY saga-common saga-common
COPY order-service order-service
COPY payment-service payment-service
COPY inventory-service inventory-service
COPY saga-e2e/pom.xml saga-e2e/pom.xml
# The service and the modules it depends on; the Maven repository is cached between builds
RUN --mount=type=cache,target=/root/.m2 chmod +x mvnw && ./mvnw -B -q -pl ${SERVICE} -am package -DskipTests

# --- Extract layers ------------------------------------------------------------
FROM eclipse-temurin:21-jdk-alpine AS extract
ARG SERVICE
WORKDIR /extract
COPY --from=build /workspace/${SERVICE}/target/${SERVICE}-*-exec.jar app.jar
RUN java -Djarmode=tools -jar app.jar extract --layers --launcher --destination layers

# --- Runtime -------------------------------------------------------------------
FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S app && adduser -S app -G app
USER app
WORKDIR /app

COPY --from=extract /extract/layers/dependencies/ ./
COPY --from=extract /extract/layers/spring-boot-loader/ ./
COPY --from=extract /extract/layers/snapshot-dependencies/ ./
COPY --from=extract /extract/layers/application/ ./

EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "org.springframework.boot.loader.launch.JarLauncher"]
