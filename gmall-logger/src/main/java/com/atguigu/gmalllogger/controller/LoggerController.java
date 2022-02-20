package com.atguigu.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController////表示返回普通对象而不是页面
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping("test1")
    public String test(){
        System.out.println("11111");
        return "success";
    }
    @RequestMapping("applog")//因为jar包发送的地址就是applog
    public String getLogger(@RequestParam("param") String jsonStr){
//        System.out.println(jsonStr);
//        return "success";
        //方法1:
        //打印数据到控制台并且落盘                 当前类的名称
/*        Logger logger = LoggerFactory.getLogger(LoggerController.class);
        logger.info(jsonStr);*/

        //方法2:
        //打印数据带控制台使用Lombok
        log.info(jsonStr);
        //将数据发送到canal是封装成message
        kafkaTemplate.send("ods_base_log",jsonStr);

        return "success";

    }
}

