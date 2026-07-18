package ma.enset.clickproducer.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class ClickApiController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.topics.clicks}")
    private String clicksTopic;

    public ClickApiController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Reçoit un clic depuis le bouton de la page web et l'envoie vers Kafka.
     * key = userId, value = "click"
     */
    @PostMapping("/api/click")
    public ResponseEntity<Void> click(@RequestBody Map<String, String> body) {
        String userId = body.getOrDefault("userId", "user1");
        kafkaTemplate.send(clicksTopic, userId, "click");
        return ResponseEntity.ok().build();
    }
}
