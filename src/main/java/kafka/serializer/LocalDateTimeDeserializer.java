package kafka.serializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

public class LocalDateTimeDeserializer extends JsonDeserializer<LocalDateTime> {
  @Override
  public LocalDateTime deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
    String str = jsonParser.readValueAsTree().toString().replaceAll("\"", "");
    str = str.replaceAll("\\s", "T");
    if (str.endsWith("Z")) {
      return ZonedDateTime.parse(str).toLocalDateTime();
    } else {
      return LocalDateTime.parse(str);
    }
  }
}
