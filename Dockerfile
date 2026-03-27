FROM eclipse-temurin:17-jre

WORKDIR /app
COPY target/gene2life-app.jar /app/gene2life-app.jar
COPY config /app/config

ENTRYPOINT ["java", "-jar", "/app/gene2life-app.jar"]
