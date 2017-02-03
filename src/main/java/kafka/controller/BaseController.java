package kafka.controller;

import kafka.kafkaProducer.KafkaProducer;
import lombok.extern.log4j.Log4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;

@Controller
@Log4j
public class BaseController {

  @RequestMapping(value = "/", method = RequestMethod.GET)
  public ModelAndView home(HttpServletRequest request) {
    log.debug("mapped to / ");
    ModelAndView mav = new ModelAndView("index");
    return mav;
  }

  @Autowired
  KafkaProducer kafkaProducer;

  /*@RequestMapping(value = "/topic/put/", method = RequestMethod.GET)
  @ResponseBody
  public String test(
      @RequestParam String topicName,
      @RequestParam(required = false) String partition,
      @RequestParam String message,
      HttpServletRequest request) throws JsonProcessingException, InterruptedException, ExecutionException {
    System.out.println("mapped to " + request.getServletPath());
    SendDto sendDto = new SendDto();
    sendDto.setString(message);
    sendDto.setLocalDateTime(LocalDateTime.now());
    SendDto.NestedObj nestedObj = new SendDto.NestedObj();
    nestedObj.setNestBoolean(true);
    nestedObj.setNestInteger(100);
    nestedObj.setNestString("this is nest " + message);
    sendDto.setNestedObj(nestedObj);
    int partitionNumber = StringUtils.isEmpty(partition) ? 0 : Integer.valueOf(partition);
    return kafkaProducer.sendJson(topicName, partitionNumber, sendDto);
  }*/

}

