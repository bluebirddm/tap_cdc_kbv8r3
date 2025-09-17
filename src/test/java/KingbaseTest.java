import io.debezium.connector.kingbasees.PostgresConnector;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.relational.history.FileDatabaseHistory;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KingbaseTest {

  private static DebeziumEngine<ChangeEvent<String, String>> engine;

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.setProperty("name", "kingbase");
    props.setProperty("connector.class", PostgresConnector.class.getName());
    props.setProperty("offset.storage", FileOffsetBackingStore.class.getName());
    props.setProperty("offset.storage.file.filename", "/user/dm/code/taiji/cdc/debezium-kingbase/offset.txt");
    props.setProperty("offset.flush.interval.ms", "60000");

    props.setProperty("snapshot.mode", "initial");
    props.setProperty("plugin.name", "decoderbufs");

    props.setProperty("database.hostname", "localhost");
    props.setProperty("database.port", "54321");
    props.setProperty("database.user", "debezium_user");
      props.setProperty("database.password", "debezium_password");
    props.setProperty("database.server.id", "85701");
    props.setProperty("database.history", FileDatabaseHistory.class.getCanonicalName());
    props.setProperty("database.history.file.filename", "/user/dm/code/taiji/cdc/debezium-kingbase/history.txt");
    props.setProperty("database.server.name", "kingbase-server");
    props.setProperty("database.dbname", "test");
    props.setProperty("table.include.list", "public.users");

    engine =
        DebeziumEngine.create(Json.class)
            .using(props)
            .notifying(
                record -> {
                  System.out.println(record);
                })
            .using(
                (success, message, error) -> {
                  if (!success && error != null) {
                    System.out.println("----------error------");
                    System.out.println(message);
                    System.out.println(error);
                    error.printStackTrace();
                  }
                  closeEngine(engine);
                })
            .build();

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.execute(engine);
    addShutdownHook(engine);
    awaitTermination(executor);
    System.out.println("------------main finished.");
  }

  private static void closeEngine(DebeziumEngine<ChangeEvent<String, String>> engine) {
    try {
      engine.close();
    } catch (IOException ignored) {
    }
  }

  private static void addShutdownHook(DebeziumEngine<ChangeEvent<String, String>> engine) {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> closeEngine(engine)));
  }

  private static void awaitTermination(ExecutorService executor) {
    if (executor != null) {
      try {
        executor.shutdown();
        while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {}
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
