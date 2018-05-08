package com.ascend.spring;

import lombok.extern.slf4j.Slf4j;
import net.sf.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;

@Controller
@Slf4j
public class SpringKafkaController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @ResponseBody
    @RequestMapping(value = "/trade_entrust", method = RequestMethod.POST, consumes = "application/json")
    public void signIn(HttpServletRequest request, @RequestBody JSONObject params, HttpServletResponse response) {
        PrintWriter writer = null;
        String rspMsg = "委托失败";
        try {
            writer = response.getWriter();
            String entrustInfo = params.toString();
            if (StringUtils.isNotBlank(entrustInfo)) {
                kafkaTemplate.sendDefault(entrustInfo);
                rspMsg = "委托成功";
            } else {
                rspMsg = "请求参数非法";
            }
        } catch (Exception e) {
            rspMsg = "消息发送失败";
            log.error(rspMsg, e);
        } finally {
            writer.append(rspMsg);
            if (writer != null) {
                writer.close();
            }
        }
    }
}
