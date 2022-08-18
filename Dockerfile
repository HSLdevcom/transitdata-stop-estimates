FROM eclipse-temurin:11-alpine
#Install curl for health check
RUN apk add --no-cache curl

ADD target/transitdata-stop-estimates.jar /usr/app/transitdata-stop-estimates.jar
COPY start-application.sh /
RUN chmod +x /start-application.sh
CMD ["/start-application.sh"]
