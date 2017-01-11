package kafka.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;

@Controller
public class BaseController {

  @RequestMapping(value = "/", method = RequestMethod.GET)
  public ModelAndView home(HttpServletRequest request) {
    System.out.println("mapped to / ");
    ModelAndView mav = new ModelAndView("index");
    return mav;
  }
}

