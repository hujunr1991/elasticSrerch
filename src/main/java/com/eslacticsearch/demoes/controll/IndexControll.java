package com.eslacticsearch.demoes.controll;


import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class IndexControll {

    @GetMapping({"/", "/index"})
    public String index() {
        return "index";
    }

}
