package ma.enset.textcleaning.config;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Exercice 1 : Nettoyage et validation de messages texte.
 *
 * text-input -> nettoyage -> validation -> text-clean / text-dead-letter
 */
@Configuration
@EnableKafkaStreams
public class TextCleaningTopology {

    private static final Logger log = LoggerFactory.getLogger(TextCleaningTopology.class);

    private static final List<String> FORBIDDEN_WORDS = List.of("HACK", "SPAM", "XXX");
    private static final int MAX_LENGTH = 100;
    private static final Pattern MULTIPLE_SPACES = Pattern.compile("\\s+");

    @Value("${app.topics.input}")
    private String inputTopic;

    @Value("${app.topics.clean}")
    private String cleanTopic;

    @Value("${app.topics.dead-letter}")
    private String deadLetterTopic;

    /**
     * Spring Kafka appelle automatiquement toute méthode @Bean qui prend un StreamsBuilder
     * en paramètre pour construire la topologie, grâce à @EnableKafkaStreams.
     */
    @Bean
    public KStream<String, String> textCleaningStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> source = streamsBuilder.stream(inputTopic);

        KStream<String, String> cleaned = source.mapValues(this::cleanMessage);

        cleaned.split()
                .branch((key, value) -> isValid(value),
                        Branched.withConsumer(validStream -> validStream.to(cleanTopic)))
                .defaultBranch(
                        Branched.withConsumer(invalidStream -> invalidStream.to(deadLetterTopic)));

        return cleaned;
    }

    /**
     * Supprime les espaces en début/fin, réduit les espaces multiples à un seul,
     * puis met en majuscules.
     */
    private String cleanMessage(String raw) {
        if (raw == null) {
            return "";
        }
        String trimmed = raw.trim();
        String singleSpaced = MULTIPLE_SPACES.matcher(trimmed).replaceAll(" ");
        return singleSpaced.toUpperCase();
    }

    /**
     * Un message nettoyé est valide s'il n'est pas vide, ne dépasse pas 100 caractères
     * et ne contient aucun mot interdit (recherche faite sur le texte déjà en majuscules).
     */
    private boolean isValid(String cleanedMessage) {
        if (cleanedMessage.isEmpty()) {
            log.debug("Message rejeté: vide ou uniquement des espaces");
            return false;
        }
        if (cleanedMessage.length() > MAX_LENGTH) {
            log.debug("Message rejeté: dépasse {} caractères", MAX_LENGTH);
            return false;
        }
        for (String forbidden : FORBIDDEN_WORDS) {
            if (cleanedMessage.contains(forbidden)) {
                log.debug("Message rejeté: contient le mot interdit '{}'", forbidden);
                return false;
            }
        }
        return true;
    }
}
