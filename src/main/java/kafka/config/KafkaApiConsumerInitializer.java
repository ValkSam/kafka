package kafka.config;

import kafka.exception.IllegalKeyMappingMethodException;
import kafka.kafkaListener.KafkaListener;
import kafka.kafkaListener.ListenerContainer;
import kafka.kafkaListener.annotation.KafkaMapping;
import kafka.kafkaListener.handler.MessageHandler;
import kafka.kafkaListener.task.KafkaMessageProcessTask;
import kafka.util.LogMessage;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

import javax.annotation.PostConstruct;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

@Configuration
@PropertySource(value = {"classpath:/kafkaJob.properties"})
@Log4j
public class KafkaApiConsumerInitializer {

  @PostConstruct
  public void init() throws IllegalKeyMappingMethodException {
    MessageHandler messageHandler = messageHandler();
    /**/
    Method[] methods = messageHandler.getClass().getDeclaredMethods();
    ListenerContainer listenerContainer = listenerContainer();
    for (Method method : methods) {
      if (method.isAnnotationPresent(KafkaMapping.class)) {
        KafkaMapping kafkaMapping = method.getAnnotation(KafkaMapping.class);
        listenerContainer.addListener(
            kafkaMapping.topic(),
            kafkaMapping.key(),
            kafkaMapping.listenerName(),
            new Function<ConsumerRecord<String, String>, Boolean>() {
              @Override
              public Boolean apply(ConsumerRecord<String, String> record) {
                try {
                  method.invoke(messageHandler, method.getName(), record);
                  return true;
                } catch (Exception e) {
                  return false;
                }
              }
            });
      }
    }
    for (Map.Entry<String, KafkaListener> pair : listenerContainer.getListenerList().entrySet()) {
      KafkaListener kafkaListener = pair.getValue();
      log.debug(LogMessage.logMessage("bind listener for", kafkaListener.getTopic()));
      listenerContainer.getKafkaListenerExecutor().submit(kafkaListener);
    }
  }

  @Bean
  public MessageHandler messageHandler() {
    return new MessageHandler();
  }

  @Bean
  @Scope("prototype")
  public KafkaMessageProcessTask kafkaMessageProcessTask() {
    return new KafkaMessageProcessTask();
  }

  @Bean
  @Scope("prototype")
  public KafkaListener kafkaListener() {
    return new KafkaListener() {
      @Override
      public KafkaMessageProcessTask getKafkaMessageProcessTask(
          ConsumerRecord<String, String> record,
          String submittedByListenerName,
          Function<ConsumerRecord<String, String>, Boolean> topicMappedMethodExecutor,
          Consumer onSuccess,
          Consumer onError) {
        KafkaMessageProcessTask kafkaMessageProcessTask = kafkaMessageProcessTask();
        kafkaMessageProcessTask.setRecord(record);
        kafkaMessageProcessTask.setListenerName(submittedByListenerName);
        kafkaMessageProcessTask.setTopicMappedMethodExecutor(topicMappedMethodExecutor);
        kafkaMessageProcessTask.setOnSuccess(onSuccess);
        kafkaMessageProcessTask.setOnError(onError);
        return kafkaMessageProcessTask;
      }
    };
  }

  @Bean
  public ListenerContainer listenerContainer() {
    return new ListenerContainer() {
      @Override
      public KafkaListener getKafkaListener() {
        return kafkaListener();
      }

      @Override
      public KafkaListener getKafkaListener(String listenerName) {
        KafkaListener kafkaListener = kafkaListener();
        kafkaListener.setName(listenerName);
        return kafkaListener;
      }
    };
  }

  @Bean
  /*
  * https://kafka.apache.org/documentation/#consumerapi
  * */
  public Properties consumerProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "consumer-tutorial-5");
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());
    props.put("enable.auto.commit", "false");
    props.put("auto.offset.reset", "latest");
//    props.put("auto.offset.reset", "earliest");
    return props;
  }

}


