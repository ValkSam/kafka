package kafka.kafkaListener.task;

import kafka.util.LogMessage;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

@Log4j
@Setter
public class KafkaMessageProcessTask implements Callable<String> {

  private Consumer<ConsumerRecord<String, String>> topicMappedMethodExecutor;

  private ConsumerRecord<String, String> record;
  private String listenerName;

  @Override
  public String call() throws Exception {
    log.debug(LogMessage.logMessage("started by listener " + listenerName + " task for", record));
    topicMappedMethodExecutor.accept(record);
    log.debug(LogMessage.logMessage("finished of listener " + listenerName + " of task for", record));
    return null;
  }

}
