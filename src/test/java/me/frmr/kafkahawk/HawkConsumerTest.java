package me.frmr.kafkahawk;

import java.io.IOException;
import java.util.Set;
import kafka.coordinator.group.*;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public class HawkConsumerTest {
  @AfterEach
  void clearMetricChildren() {
    HawkConsumer.commitsCounter.clear();
    HawkConsumer.commitDeltas.clear();
  }

  @Test
  void consumeSingleRecordCommitCounter() throws NoSuchMethodException, IOException {
    var key = getClass().getClassLoader().getResourceAsStream("key_0.dat").readAllBytes();
    var value = getClass().getClassLoader().getResourceAsStream("msg_0.dat").readAllBytes();

    var record = new ConsumerRecord<byte[], byte[]>("__consumer_offsets", 0, 0l, key, value);

    var sut = new HawkConsumer(null, false, null, "");

    sut.consumeSingleRecord(record);

    assertEquals(1.0, HawkConsumer.commitsCounter.labels("", "console-consumer-45944", "test").get());
  }

  @Test
  void consumeSingleRecordDeltaCounter() throws NoSuchMethodException, IOException {
    var key = getClass().getClassLoader().getResourceAsStream("key_0.dat").readAllBytes();
    var value = getClass().getClassLoader().getResourceAsStream("msg_0.dat").readAllBytes();

    var record = new ConsumerRecord<byte[], byte[]>("__consumer_offsets", 0, 0l, key, value);

    var sut = new HawkConsumer(null, true, Set.of("console-consumer-45944"), "");

    sut.consumeSingleRecord(record);

    assertNotNull(sut.lastOffsets.get("console-consumer-45944"));
    assertNotNull(sut.lastOffsets.get("console-consumer-45944").get(new TopicPartition("test", 0)));

    var recordedOffset = sut.lastOffsets.get("console-consumer-45944").get(new TopicPartition("test", 0));
    assertEquals(Long.valueOf(0), recordedOffset);
    assertEquals(0.0, HawkConsumer.commitDeltas.labels("", "console-consumer-45944", "test", "0").get());
  }

}
