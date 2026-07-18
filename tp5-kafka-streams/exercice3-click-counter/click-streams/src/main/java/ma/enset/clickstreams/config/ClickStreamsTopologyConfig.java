package ma.enset.clickstreams.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * Exercice 3 - Partie 2 : comptage des clics.
 *
 * Implémentation par défaut : Variante 2 (comptage par utilisateur).
 * clicks (key=userId, value="click") -> groupByKey().count() -> click-counts (key=userId, value=count)
 */
@Configuration
@EnableKafkaStreams
public class ClickStreamsTopologyConfig {

    @Value("${app.topics.clicks}")
    private String clicksTopic;

    @Value("${app.topics.click-counts}")
    private String clickCountsTopic;

    @Bean
    public KStream<String, String> clickCountStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> clicks =
                streamsBuilder.stream(clicksTopic, Consumed.with(Serdes.String(), Serdes.String()));

        // ---- Variante 2 : comptage par utilisateur (clé = userId) ----
        KTable<String, Long> countsPerUser = clicks
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count();

        countsPerUser.toStream()
                .mapValues(String::valueOf)
                .to(clickCountsTopic, Produced.with(Serdes.String(), Serdes.String()));

        // ---- Variante 1 (comptage global) : pour l'activer, remplacer le bloc ci-dessus par :
        //
        // KTable<String, Long> globalCount = clicks
        //         .selectKey((userId, value) -> "ALL")
        //         .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        //         .count();
        //
        // globalCount.toStream()
        //         .mapValues(String::valueOf)
        //         .to(clickCountsTopic, Produced.with(Serdes.String(), Serdes.String()));

        return clicks;
    }
}
