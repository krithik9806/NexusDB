FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

COPY nexus-server/target/nexus-server-0.1.0-SNAPSHOT.jar /app/nexus-server.jar
COPY nexus-server/target/dependency/ /app/lib/

ENV JAVA_OPTS="-Xms256m -Xmx512m"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -cp /app/nexus-server.jar:/app/lib/* io.nexusdb.server.NexusNode \"$@\"", "--"]
