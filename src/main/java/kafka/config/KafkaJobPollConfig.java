package kafka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

@Configuration
@PropertySource(value = {"classpath:/kafkaJob.properties"})
public class KafkaJobPollConfig extends WebMvcConfigurerAdapter {

  @Value("${kpte.poolSize}")
  private Integer KAFKA_POLLRESUME_EXECUTOR_POOL_SIZE;
  @Value("${kpte.queueSize}")
  private Integer KAFKA_POLLRESUME_EXECUTOR_QUEUE_SIZE;
  @Value("${kpte.aliveTimeSec}")
  private Integer KAFKA_POLLRESUME_EXECUTOR_ALIVE_TIME_SECONDS;

  @Bean(name = "kafkaMessageProcessTaskExecutor")
  public ThreadPoolTaskExecutor kafkaMessageProcessTaskExecutor() {
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    threadPoolTaskExecutor.setCorePoolSize(KAFKA_POLLRESUME_EXECUTOR_POOL_SIZE);
    threadPoolTaskExecutor.setQueueCapacity(KAFKA_POLLRESUME_EXECUTOR_QUEUE_SIZE);
    threadPoolTaskExecutor.setKeepAliveSeconds(KAFKA_POLLRESUME_EXECUTOR_ALIVE_TIME_SECONDS);
    threadPoolTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
    return threadPoolTaskExecutor;
  }

  @Bean(name = "kafkaListenExecutor")
  public ThreadPoolTaskExecutor kafkaListenExecutor() {
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    threadPoolTaskExecutor.setCorePoolSize(KAFKA_POLLRESUME_EXECUTOR_POOL_SIZE);
    threadPoolTaskExecutor.setQueueCapacity(KAFKA_POLLRESUME_EXECUTOR_QUEUE_SIZE);
    threadPoolTaskExecutor.setKeepAliveSeconds(KAFKA_POLLRESUME_EXECUTOR_ALIVE_TIME_SECONDS);
    threadPoolTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
    return threadPoolTaskExecutor;
  }

}
