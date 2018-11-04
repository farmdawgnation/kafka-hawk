package me.frmr.kafkahawk;

import io.prometheus.client.Counter;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.*;
import kafka.coordinator.group.GroupMetadataManager;
import kafka.coordinator.group.OffsetKey;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.clients.consumer.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * HawkConsumer consumes messages from __consumer_offsets and tracks information
 * about the number of requests received per consumer groups in prometheus
 * counters.
 *
 * This consumer never commits offsets.
 */
public class HawkConsumer implements AutoCloseable {
  public static Logger logger = LoggerFactory.getLogger(HawkConsumer.class);

  static final Counter commitsCounter = Counter.build()
    .name("kafka_hawk_offset_commits_total")
    .help("Total number of offset commits")
    .labelNames("consumer_group", "topic", "partition")
    .register();

  ExecutorService consumerExecService = Executors.newSingleThreadExecutor();
  Map<String, Object> consumerProps;

  volatile KafkaConsumer<byte[], byte[]> currentConsumer;

  public HawkConsumer(Map<String,Object> properties) {
    consumerProps = properties;
  }

  void consume() {
    Thread.currentThread().setName("hawk-consumer-thread");
    logger.info("Hawk consumer thread is starting up");

    try (
      var consumer = new KafkaConsumer<byte[], byte[]>(consumerProps);
    ) {
      currentConsumer = consumer;

      while(true) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(250);

        logger.debug("Consumed {} records", records.count());

        for (ConsumerRecord<byte[], byte[]> record : records) {
          OffsetKey messageKey = (OffsetKey)GroupMetadataManager.readMessageKey(ByteBuffer.wrap(record.key()));
          commitsCounter.labels(
            messageKey.key().group(),
            messageKey.key().topicPartition().topic(),
            Integer.toString(messageKey.key().topicPartition().partition())
          ).inc();
        }
      }
    } catch(WakeupException wakeup) {
      logger.info("Received wakeup, terminating consumer");
    } catch(Exception ex) {
      logger.error("Unexpected exception on consumer thread", ex);
    } finally {
      currentConsumer = null;
    }
  }

  public void start() {
    if (currentConsumer == null) {
      consumerExecService.submit(this::consume);
    } else {
      throw new IllegalStateException("Start was invoked after the consumer was already running");
    }
  }

  public void stop() {
    if (currentConsumer != null) {
      currentConsumer.wakeup();
    }
  }

  public void close() {
    try {
      if (currentConsumer != null) {
        currentConsumer.wakeup();
      }

      consumerExecService.shutdown();
      consumerExecService.awaitTermination(60, TimeUnit.SECONDS);
    } catch (Exception ex) {
      logger.warn("Exception during shutdown", ex);
    }
  }
}
