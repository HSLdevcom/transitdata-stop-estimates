FROM openjdk:8-jre-slim
#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl
ADD target/transitdata-stop-estimates.jar /usr/app/transitdata-stop-estimates.jar
ENTRYPOINT ["java", "-jar", "/usr/app/transitdata-stop-estimates.jar"]