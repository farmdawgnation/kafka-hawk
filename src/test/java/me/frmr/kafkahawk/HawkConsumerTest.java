package me.frmr.kafkahawk;

import java.io.IOException;
import kafka.coordinator.group.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public class HawkConsumerTest {
  @Test
  void consumeSingleRecordTest() throws NoSuchMethodException, IOException {
    var key = getClass().getClassLoader().getResourceAsStream("key_0.dat").readAllBytes();
    var value = getClass().getClassLoader().getResourceAsStream("msg_0.dat").readAllBytes();

    var record = new ConsumerRecord<byte[], byte[]>("__consumer_offsets", 0, 0l, key, value);

    var sut = new HawkConsumer(null, false, null);

    sut.consumeSingleRecord(record);

    assertEquals(1.0, HawkConsumer.commitsCounter.labels("console-consumer-45944", "test").get());
  }
}
