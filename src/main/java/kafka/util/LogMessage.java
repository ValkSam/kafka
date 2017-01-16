package kafka.util;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

@Log4j
public class LogMessage {

  public static String logMessage(String topic, String sentMessage) {
    try {
      return "\n\t"
          .concat(topic)
          .concat(": ").concat(sentMessage)
          .concat("\n\t");
    } catch (Exception e) {
      log.error(ExceptionUtils.getStackTrace(e));
      return "error while logging";
    }
  }

  public static String logMessage(String topic, Object object) {
    return logMessage(topic, object, false);
  }

  public static String logMessage(String topic, Object object, boolean showRoot) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      if (showRoot) {
        objectMapper.enable(SerializationFeature.WRAP_ROOT_VALUE);
      }
      objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
      return "\n\t"
          .concat(topic)
          .concat(": ").concat(objectMapper.writeValueAsString(object))
          .concat("\n\t");
    } catch (Exception e) {
      log.error(ExceptionUtils.getStackTrace(e));
      return "error while logging";
    }
  }

}
