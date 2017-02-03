package kafka.kafkaListener.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaMapping {
  String topic();

  String key() default "";

  String listenerName() default "";
}
