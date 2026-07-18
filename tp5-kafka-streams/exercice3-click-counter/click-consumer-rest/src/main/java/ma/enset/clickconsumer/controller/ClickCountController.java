package ma.enset.clickconsumer.controller;

import ma.enset.clickconsumer.store.ClickCountStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/clicks")
public class ClickCountController {

    private final ClickCountStore store;

    public ClickCountController(ClickCountStore store) {
        this.store = store;
    }

    /**
     * Comptage par utilisateur, ex: GET /clicks/count -> {"user1": 45, "user2": 30}
     */
    @GetMapping("/count")
    public Map<String, Long> count() {
        return store.getAll();
    }

    /**
     * Comptage global, ex: GET /clicks/count/total -> {"totalClicks": 125}
     */
    @GetMapping("/count/total")
    public Map<String, Long> total() {
        return Map.of("totalClicks", store.getTotal());
    }
}
