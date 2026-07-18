package ma.enset.clickconsumer.listener;

import ma.enset.clickconsumer.store.ClickCountStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ClickCountListener {

    private static final Logger log = LoggerFactory.getLogger(ClickCountListener.class);

    private final ClickCountStore store;

    public ClickCountListener(ClickCountStore store) {
        this.store = store;
    }

    @KafkaListener(topics = "${app.topics.click-counts}", groupId = "${spring.kafka.consumer.group-id}")
    public void onClickCount(ConsumerRecord<String, String> record) {
        try {
            String userId = record.key();
            long count = Long.parseLong(record.value());
            store.update(userId, count);
            log.debug("Compteur mis à jour: {} = {}", userId, count);
        } catch (NumberFormatException e) {
            log.warn("Valeur de compteur invalide reçue: {}", record.value());
        }
    }
}
