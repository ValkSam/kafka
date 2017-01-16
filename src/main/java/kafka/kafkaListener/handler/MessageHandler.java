package kafka.kafkaListener.handler;

import kafka.kafkaListener.annotation.KafkaMapping;
import kafka.kafkaListener.annotation.KafkaProcessHandler;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by ValkSam on 31.01.2017.
 */
@Log4j
@KafkaProcessHandler
public class MessageHandler {

  @KafkaMapping(topic = "topic5_0_1_2")
  public void procTopic5_0_1_2(String methodName, ConsumerRecord<String, String> record) throws InterruptedException {
    String topic = record.topic();
    String key = record.key();
    String value = record.value();
    log.debug("\n\texecuting method: " + methodName + " for topic: " + topic + " for key " + key + " for message: " + value);
    Thread.sleep(6000);
  }

  @KafkaMapping(topic = "topic-repl2-broker0-1", key = "1", listenerName = "listener_1")
  public void procTopic3_0_1_key1(String methodName, ConsumerRecord<String, String> record) throws InterruptedException {
    String topic = record.topic();
    String key = record.key();
    String value = record.value();
    log.debug("\n\texecuting method: " + methodName + " for topic: " + topic + " for key " + key + " for message: " + value);
    Thread.sleep(6000);
  }

  @KafkaMapping(topic = "topic-repl2-broker0-1", key = "2", listenerName = "listener_1")
  public void procTopic3_0_1_key2(String methodName, ConsumerRecord<String, String> record) throws InterruptedException {
    String topic = record.topic();
    String key = record.key();
    String value = record.value();
    log.debug("\n\texecuting method: " + methodName + " for topic: " + topic + " for key " + key + " for message: " + value);
    Thread.sleep(6000);
  }

}
