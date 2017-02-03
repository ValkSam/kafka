package kafka.kafkaListener.task;

import kafka.exception.ErrorWhileMappedMethodExecutedException;
import kafka.util.LogMessage;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.Consumer;
import java.util.function.Function;

@Log4j
@Setter
public class KafkaMessageProcessTask implements Runnable {

  private Function<ConsumerRecord<String, String>, Boolean> topicMappedMethodExecutor;

  Consumer onSuccess;
  Consumer onError;

  private ConsumerRecord<String, String> record;
  private String listenerName;

  @Override
  public void run() {
    log.debug(LogMessage.logMessage("started by listener " + listenerName + " task for", record));
    try {
      if (!topicMappedMethodExecutor.apply(record)) {
        throw new ErrorWhileMappedMethodExecutedException();
      }
      log.debug(LogMessage.logMessage("finished of listener " + listenerName + " of task for", record));
      if (onSuccess != null) {
        onSuccess.accept(record);
      }
    } catch (Exception e) {
      log.error(LogMessage.logMessage("ERROR of listener " + listenerName + " :\n" + ExceptionUtils.getStackTrace(e), record));
      if (onError != null) {
        onError.accept(record);
      }
    }
  }

}
