package me.frmr.kafkahawk;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import kafka.coordinator.group.GroupMetadataManager;
import kafka.coordinator.group.OffsetKey;
import org.apache.kafka.common.TopicPartition;
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
    .labelNames("cluster", "consumer_group", "topic")
    .register();

  static final Gauge commitDeltas = Gauge.build()
    .name("kafka_hawk_offset_commit_detlas")
    .help("Delta between committed offsets between the previous and latest commits")
    .labelNames("cluster", "consumer_group", "topic", "partition")
    .register();

  ExecutorService consumerExecService = Executors.newSingleThreadExecutor();
  Map<String, Object> consumerProps;
  boolean deltasEnabled = false;
  Set<String> deltaGroups;
  Map<String, Map<TopicPartition, Long>> lastOffsets;
  String clusterName;

  volatile KafkaConsumer<byte[], byte[]> currentConsumer;

  /**
   * @param properties Properties to use when building the consumer
   * @param deltasEnabled Whether or not the deltas feature is enabled
   * @param deltaTopics Topics for which offset deltas will be tracked
   */
  public HawkConsumer(
      Map<String,Object> properties,
      boolean deltasEnabled,
      Set<String> deltaGroups,
      String clusterName
  ) {
    consumerProps = properties;
    this.deltasEnabled = deltasEnabled;
    this.deltaGroups = deltaGroups;
    this.clusterName = clusterName;

    this.lastOffsets = new HashMap<>();
  }

  void consumeSingleRecord(ConsumerRecord<byte[], byte[]> record) {
    try {
      OffsetKey messageKey = (OffsetKey)GroupMetadataManager.readMessageKey(ByteBuffer.wrap(record.key()));
      var group = messageKey.key().group();
      var topic = messageKey.key().topicPartition().topic();

      commitsCounter.labels(clusterName, group, topic).inc();

      if (deltasEnabled && deltaGroups.contains(group)) {
        var offsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(record.value()));
        var topicPartition = messageKey.key().topicPartition();
        var partition = Integer.toString(messageKey.key().topicPartition().partition());
        var diff = 0l;

        if (lastOffsets.containsKey(group)) {
          var groupOffsets = lastOffsets.get(group);

          if (groupOffsets.containsKey(topicPartition)) {
            diff = offsetAndMetadata.offset() - groupOffsets.get(topicPartition);
          }

          groupOffsets.put(topicPartition, offsetAndMetadata.offset());
        } else {
          var groupOffsets = new HashMap<TopicPartition, Long>();
          groupOffsets.put(topicPartition, offsetAndMetadata.offset());
          lastOffsets.put(group, groupOffsets);
        }

        commitDeltas.labels(clusterName, group, topic, partition).set(diff);
      }
    } catch(ClassCastException cce) {
      logger.debug("Ignoring a thing that I couldn't interpret as an offset");
    }
  }

  void consume() {
    Thread.currentThread().setName("hawk-consumer-thread");
    logger.info("Hawk consumer thread is starting up");

    try (
      var consumer = new KafkaConsumer<byte[], byte[]>(consumerProps);
    ) {
      currentConsumer = consumer;

      consumer.subscribe(Arrays.asList("__consumer_offsets"));

      while(true) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(250);

        logger.debug("Consumed {} records", records.count());

        for (ConsumerRecord<byte[], byte[]> record : records) {
          consumeSingleRecord(record);
        }
      }
    } catch(WakeupException wakeup) {
      logger.info("Received wakeup, terminating consumer");
    } catch(Exception ex) {
      logger.error("Unexpected exception on consumer thread", ex);
      System.exit(255);
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
