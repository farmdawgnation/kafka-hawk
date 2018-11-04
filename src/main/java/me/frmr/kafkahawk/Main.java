package me.frmr.kafkahawk;

import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class Main {
  private static Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    logger.info("Kafka Hawk Booting.");

    var config = ConfigFactory.load();
    logger.info("Config loaded.");

    DefaultExports.initialize();
    logger.info("Hotspot metrics collectors started.");

    try {
      var server = new HTTPServer(config.getInt("hawk.prometheus.port"), false);
    } catch (IOException ioe) {
      logger.error("HTTPServer failed to start", ioe);
      System.exit(255);
    }

    logger.info("Prometheus HTTP server started.");

    var kafkaConfig = config.getObject("hawk.kafka").unwrapped();
    var hc = new HawkConsumer(kafkaConfig);
    hc.start();

    logger.info("HawkConsumer started");
  }
}
