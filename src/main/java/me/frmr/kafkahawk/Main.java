package me.frmr.kafkahawk;

import com.typesafe.config.ConfigFactory;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class Main {
  private static Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    logger.info("Kafka Hawk Booting.");

    var config = ConfigFactory.load();
    logger.info("Config loaded.");
  }
}
