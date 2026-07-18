package ma.enset.clickconsumer.store;

import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache en mémoire, alimenté par le topic "click-counts".
 * Simple et suffisant pour une seule instance ; pour scaler à plusieurs instances de
 * cette API, il faudrait s'appuyer sur les "interactive queries" de Kafka Streams
 * (state store distribué) plutôt que sur une Map locale.
 */
@Component
public class ClickCountStore {

    private final Map<String, Long> countsByUser = new ConcurrentHashMap<>();

    public void update(String userId, long count) {
        countsByUser.put(userId, count);
    }

    public Map<String, Long> getAll() {
        return Collections.unmodifiableMap(countsByUser);
    }

    public long getTotal() {
        return countsByUser.values().stream().mapToLong(Long::longValue).sum();
    }
}
