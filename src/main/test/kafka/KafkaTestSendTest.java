package kafka;

import kafka.config.WebAppConfig;
import kafka.kafkaProducer.KafkaProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

@WebAppConfiguration
@ContextConfiguration(classes = {WebAppConfig.class})
@RunWith(SpringJUnit4ClassRunner.class)
public class KafkaTestSendTest {
//  @Autowired
//  KafkaProducer kafkaProducer;

  @Test
  public void check() throws Exception {
//    kafkaProducer.sendJson("test", 0, "rrrrrrr");
//    Thread.sleep(2000);
  }
}
