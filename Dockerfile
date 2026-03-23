FROM eclipse-temurin:17-jre

WORKDIR /app
COPY build/classes /app/classes
COPY config /app/config

ENTRYPOINT ["java", "-cp", "/app/classes", "org.gene2life.cli.Main"]
