package kafka.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import kafka.serializer.LocalDateTimeDeserializer;
import kafka.serializer.LocalDateTimeSerializer;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDateTime;

@Setter
@ToString
public class SendDto {
  private String string;

  @JsonSerialize(using = LocalDateTimeSerializer.class)
  @JsonDeserialize(using = LocalDateTimeDeserializer.class)
  private LocalDateTime localDateTime;
  private NestedObj nestedObj;

  @Setter
  public static class NestedObj {
    private String nestString;
    private Integer nestInteger;
    private Boolean nestBoolean;
  }

}
