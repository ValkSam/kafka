package kafka.kafkaListener;

import kafka.exception.IllegalKeyMappingMethodException;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

@Component
@Getter
@Log4j
@PropertySource(value = {"classpath:/kafkaJob.properties"})
public abstract class ListenerContainer {
  @Value("${kpte.poolSize}")
  private Integer KAFKA_POLLRESUME_EXECUTOR_POOL_SIZE;
  @Value("${kpte.queueSize}")
  private Integer KAFKA_POLLRESUME_EXECUTOR_QUEUE_SIZE;
  @Value("${kpte.aliveTimeSec}")
  private Integer KAFKA_POLLRESUME_EXECUTOR_ALIVE_TIME_SECONDS;

  @Autowired
  Properties consumerProperties;

  @Autowired
  @Qualifier("kafkaListenExecutor")
  private ThreadPoolTaskExecutor kafkaListenerExecutor;

  @Autowired
  @Qualifier("kafkaMessageProcessTaskExecutor")
  private ThreadPoolTaskExecutor kafkaMessageProcessTaskExecutor;

  private ConcurrentHashMap<String, KafkaListener> listenerList = new ConcurrentHashMap<>();

  abstract public KafkaListener getKafkaListener();

  abstract public KafkaListener getKafkaListener(String listenerName);

  @PreDestroy
  private void destroy() {
    log.debug("ListenerContainer destroy started ... ");
    log.debug("kafkaListeners stop ");
    for (Map.Entry<String, KafkaListener> pair : listenerList.entrySet()) {
      pair.getValue().getKafkaConsumer().wakeup();
    }
    log.debug("kafkaListenerExecutor.shutdown ");
    if (kafkaListenerExecutor != null) {
      kafkaListenerExecutor.shutdown();
    }
    log.debug("kafkaMessageProcessTaskExecutor.shutdown ");
    if (kafkaMessageProcessTaskExecutor != null) {
      kafkaMessageProcessTaskExecutor.shutdown();
    }
    log.debug("... ListenerContainer destroy completed ");
  }

  public KafkaListener addListener(
      String topic,
      String key,
      String listenerName,
      Consumer<ConsumerRecord<String, String>> executor) throws IllegalKeyMappingMethodException {
    KafkaListener kafkaListener = listenerList.get(topic);
    if (kafkaListener == null) {
      KafkaConsumer kafkaConsumer = new KafkaConsumer<>(consumerProperties);
      kafkaConsumer.subscribe(Arrays.asList(topic));
      /**/
      kafkaListener = getKafkaListener();
      kafkaListener.setName("".equals(listenerName) ? String.valueOf(kafkaListener.hashCode()) : listenerName);
      kafkaListener.setTopic(topic);
      kafkaListener.setKafkaConsumer(kafkaConsumer);
      kafkaListener.setThreadPoolExecutor(kafkaMessageProcessTaskExecutor);
      /**/
      listenerList.put(topic, kafkaListener);
    }
    key = key == null ? "" : key;
    if (!validKey(kafkaListener, key)) {
      throw new IllegalKeyMappingMethodException(String.format("you can not map the topic %s for ALL keys and for concrete keys simultaneously !", topic));
    }
    kafkaListener.getKeyHandlerMap().put(key, executor);
    return kafkaListener;
  }

  private boolean validKey(KafkaListener kafkaListener, String key) {
    if (
        (!"".equals(key) && kafkaListener.getKeyHandlerMap().get("") != null)
            || ("".equals(key) && kafkaListener.getKeyHandlerMap().size() != 0)
        ) {
      return false;
    }
    return true;
  }

}
