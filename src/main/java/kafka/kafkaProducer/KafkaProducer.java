package kafka.kafkaProducer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.util.LogMessage;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ExecutionException;

@Component
@Log4j
public class KafkaProducer {
  @Autowired
  protected KafkaTemplate<Integer, String> kafkaTemplate;

  public String sendJson(String topic, int partition, Object object) throws JsonProcessingException, InterruptedException, ExecutionException {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    String json = objectMapper.writeValueAsString(object);
    kafkaTemplate.setProducerListener(new ProducerListener<Integer, String>() {
      @Override
      public void onSuccess(String s, Integer integer, Integer integer2, String s2, RecordMetadata recordMetadata) {
        log.debug(LogMessage.logMessage("received by set Listener", s2));
        log.debug(LogMessage.logMessage("received by set Listener", recordMetadata));
      }

      @Override
      public void onError(String s, Integer integer, Integer integer2, String s2, Exception e) {
        log.error("failed {}", e);
      }

      @Override
      public boolean isInterestedInSuccess() {
        return true;
      }
    });
    ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send(topic, partition, json);
    log.debug(LogMessage.logMessage("sent", json));
    future.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
      @Override
      public void onSuccess(SendResult<Integer, String> result) {
        log.debug(LogMessage.logMessage("delivered", result.getProducerRecord()));
      }

      @Override
      public void onFailure(Throwable ex) {
        log.error("failed {}", ex);
      }
    });
    return json;
  }

}
