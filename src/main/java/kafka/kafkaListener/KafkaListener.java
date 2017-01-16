package kafka.kafkaListener;

import kafka.kafkaListener.task.KafkaMessageProcessTask;
import kafka.util.LogMessage;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Component
@Setter @Getter
@Log4j
public abstract class KafkaListener implements Runnable {
  private String name;
  private KafkaConsumer kafkaConsumer;
  private ThreadPoolTaskExecutor threadPoolExecutor;

  private String topic;
  private ConcurrentHashMap<String, Consumer<ConsumerRecord<String, String>>> keyHandlerMap = new ConcurrentHashMap<>();

  public abstract KafkaMessageProcessTask getKafkaMessageProcessTask(ConsumerRecord<String, String> record, String submittedByListenerName, Consumer<ConsumerRecord<String, String>> topicMappedMethodExecutor);

  @Override
  public void run() {
    log.debug("start listener " + name + " for consumer " + kafkaConsumer.hashCode() + " for topic: " + topic);
    try {
      while (true) {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Long.MAX_VALUE);
        if (!records.isEmpty()) {
          log.debug(LogMessage.logMessage("kafka api get records by listener: " + name, records));
          for (ConsumerRecord<String, String> record : records) {
            String key = record.key();
            Consumer topicMappedMethodExecutor = getTopicMappedMethodExecutor(key);
            if (topicMappedMethodExecutor != null) {
              log.debug(LogMessage.logMessage("kafka api process record  by listener: " + name + " for key: " + key, record));
              KafkaMessageProcessTask task = getKafkaMessageProcessTask(
                  record,
                  name,
                  topicMappedMethodExecutor);
              threadPoolExecutor.submit(task);
            } else {
              log.debug(LogMessage.logMessage("kafka api listener: " + name + " missed message for key: " + key, record));
            }
          }
        }
      }
    } catch (WakeupException e) {
      log.debug("interrupted consumer " + kafkaConsumer.hashCode() + " for topic: " + topic);
    } catch (Exception e) {
      log.error("error for consumer " + kafkaConsumer.hashCode() + " for topic: " + topic);
      log.error(ExceptionUtils.getStackTrace(e));
      throw e;
    } finally {
      log.debug("closed consumer " + kafkaConsumer.hashCode() + " for topic: " + topic);
      kafkaConsumer.close();
    }
  }

  private Consumer getTopicMappedMethodExecutor(String key) {
    if ((keyHandlerMap.size() == 1) && (keyHandlerMap.get("") != null)) {
      return keyHandlerMap.get("");
    }
    return keyHandlerMap.get(key);
  }

}
