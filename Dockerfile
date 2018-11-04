FROM openjdk:11

COPY build/libs/kafka-hawk-uber-*.jar /kafka-hawk.jar

ENTRYPOINT ["java", "-jar", "/kafka-hawk.jar"]
