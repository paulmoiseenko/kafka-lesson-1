FROM gradle:8.7-jdk21 AS build
WORKDIR /home/gradle/app
COPY . .
RUN gradle build -x test

FROM bellsoft/liberica-openjdk-alpine-musl
WORKDIR /app
COPY --from=build /home/gradle/app/build/libs/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
