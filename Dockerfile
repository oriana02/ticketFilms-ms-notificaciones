# ---- build ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

# ---- run ----
FROM eclipse-temurin:21-jre
WORKDIR /app

# Usuario no-root
RUN groupadd -r app && useradd -r -g app app

COPY --from=build /app/target/*.jar app.jar
RUN chown app:app app.jar

USER app

# Sin EXPOSE: worker sin puerto HTTP.
# Sin HEALTHCHECK HTTP: no hay actuator/web. El compose usa restart: unless-stopped.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0"

ENTRYPOINT ["java", "-jar", "app.jar"]