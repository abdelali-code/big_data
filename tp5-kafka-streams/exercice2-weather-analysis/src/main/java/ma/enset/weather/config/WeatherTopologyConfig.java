package ma.enset.weather.config;

import ma.enset.weather.model.WeatherReading;
import ma.enset.weather.model.WeatherStats;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * Exercice 2 : Analyse de données météorologiques.
 *
 * weather-data (CSV) -> parse -> filtre (T > 30) -> conversion F -> groupBy(station)
 * -> aggregate(moyennes) -> station-averages
 */
@Configuration
@EnableKafkaStreams
public class WeatherTopologyConfig {

    private static final Logger log = LoggerFactory.getLogger(WeatherTopologyConfig.class);

    @Value("${app.topics.input}")
    private String inputTopic;

    @Value("${app.topics.output}")
    private String outputTopic;

    @Value("${app.temperature-threshold-celsius:30.0}")
    private double temperatureThreshold;

    @Bean
    public KStream<String, String> weatherAveragesStream(StreamsBuilder streamsBuilder) {
        JsonSerde<WeatherReading> readingSerde = new JsonSerde<>(WeatherReading.class);
        JsonSerde<WeatherStats> statsSerde = new JsonSerde<>(WeatherStats.class);

        KStream<String, String> rawLines = streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        // Parsing tolérant : une ligne mal formée devient null puis est filtrée,
        // sans jamais interrompre le traitement des autres messages.
        KStream<String, WeatherReading> parsed = rawLines
                .mapValues(line -> {
                    WeatherReading reading = WeatherReading.parse(line);
                    if (reading == null) {
                        log.warn("Message mal formé ignoré: '{}'", line);
                    }
                    return reading;
                })
                .filter((key, reading) -> reading != null);

        KStream<String, WeatherReading> aboveThreshold =
                parsed.filter((key, reading) -> reading.getTemperature() > temperatureThreshold);

        KStream<String, WeatherReading> inFahrenheit =
                aboveThreshold.mapValues(WeatherReading::toFahrenheit);

        // La station devient la clé du message pour permettre le regroupement
        KStream<String, WeatherReading> keyedByStation =
                inFahrenheit.selectKey((key, reading) -> reading.getStation());

        KTable<String, WeatherStats> stationAverages = keyedByStation
                .groupByKey(Grouped.with(Serdes.String(), readingSerde))
                .aggregate(
                        WeatherStats::new,
                        (station, reading, stats) -> stats.add(reading),
                        Materialized.<String, WeatherStats>as("station-averages-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(statsSerde)
                );

        KStream<String, String> formatted = stationAverages
                .toStream()
                .mapValues(WeatherStats::toFormattedString);

        formatted.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        return rawLines;
    }
}
