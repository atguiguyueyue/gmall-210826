package com.atguigu.demo.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//@Controller+ @ResponseBody=@RestController
@RestController
public class TestController {

    @RequestMapping("test")
//    @ResponseBody
    public String test1(){
        System.out.println("123");
        return "success";
    }

    @RequestMapping("test1")
    public String test2(@RequestParam("name") String na, @RequestParam("age") Integer age){
        System.out.println("aaa");
        return "name:" + na + " age:" + age;
    }

}
