package kafka.kafkaListener;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.dto.SendDto;
import kafka.util.LogMessage;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Log4j
public class Listener {
  @KafkaListener(topicPattern = "test.*")
  public void listenTest(String message) throws IOException, InterruptedException {
    log.debug(LogMessage.logMessage("test: received raw", message));
    ObjectMapper objectMapper = new ObjectMapper();
    SendDto sendDto = objectMapper.readerFor(SendDto.class).readValue(message);
    log.debug(LogMessage.logMessage("received object", sendDto, true));
  }


  @KafkaListener(topicPattern = "topic2.*")
  public void listenMyRreplicatedTopic(ConsumerRecord<?, ?> record) throws IOException, InterruptedException {
    log.debug(LogMessage.logMessage("all", record));
  }

  @KafkaListener(topicPartitions =
      {
          @TopicPartition(topic = "topic3_0_1", partitions = {"0"})
      })
  public void listenMyRreplicatedTopic1_0(ConsumerRecord<?, ?> record) throws IOException, InterruptedException {
    log.debug(LogMessage.logMessage("topic3_0_1/0", record));
  }

  @KafkaListener(topicPartitions =
      {
          @TopicPartition(topic = "topic3_0_1", partitions = {"1"})
      })
  public void listenMyRreplicatedTopic1(ConsumerRecord<?, ?> record) throws IOException, InterruptedException {
    log.debug(LogMessage.logMessage("topic3_0_1/1", record));
  }

  @KafkaListener(topicPartitions =
      {
          @TopicPartition(topic = "topic3_0_1", partitions = {"2"})
      })
  public void listenMyRreplicatedTopic2(ConsumerRecord<?, ?> record) throws IOException, InterruptedException {
    log.debug(LogMessage.logMessage("topic3_0_1/2", record));
  }

  @KafkaListener(topicPartitions =
      {
          @TopicPartition(topic = "topic5_0_1_2", partitions = {"0"})
      })
  public void listenMyRreplicatedTopic3(ConsumerRecord<?, ?> record) throws IOException, InterruptedException {
    log.debug(LogMessage.logMessage("topic5_0_1_2/0", record));
  }

  @KafkaListener(topicPartitions =
      {
          @TopicPartition(topic = "topic5_0_1_2", partitions = {"1"})
      })
  public void listenMyRreplicatedTopic4(ConsumerRecord<?, ?> record) throws IOException, InterruptedException {
    log.debug(LogMessage.logMessage("topic5_0_1_2/1", record));
  }

  @KafkaListener(topicPartitions =
      {
          @TopicPartition(topic = "topic5_0_1_2", partitions = {"2"})
      })
  public void listenMyRreplicatedTopic5(ConsumerRecord<?, ?> record) throws IOException, InterruptedException {
    log.debug(LogMessage.logMessage("topic5_0_1_2/2", record));
  }

}
