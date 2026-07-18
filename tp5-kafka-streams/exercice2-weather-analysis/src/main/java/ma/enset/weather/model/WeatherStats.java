package ma.enset.weather.model;

/**
 * Résultat d'agrégation : compteur + sommes, pour calculer des moyennes glissantes
 * par station au fil des messages (sémantique KTable).
 */
public class WeatherStats {

    private String station;
    private long count;
    private double sumTemperature;
    private double sumHumidity;

    public WeatherStats() {
    }

    /**
     * Ajoute une nouvelle mesure (déjà en Fahrenheit) aux totaux courants.
     * Utilisé comme "aggregator" dans le .aggregate() de Kafka Streams.
     */
    public WeatherStats add(WeatherReading reading) {
        this.station = reading.getStation();
        this.count += 1;
        this.sumTemperature += reading.getTemperature();
        this.sumHumidity += reading.getHumidity();
        return this;
    }

    public double averageTemperature() {
        return count == 0 ? 0.0 : sumTemperature / count;
    }

    public double averageHumidity() {
        return count == 0 ? 0.0 : sumHumidity / count;
    }

    /**
     * Formate le résultat exactement comme demandé dans l'énoncé :
     * "Station2 : Temperature moyenne = 99.5 F , Humidite moyenne = 47.5 %"
     */
    public String toFormattedString() {
        return String.format("%s : Temperature moyenne = %.1f F , Humidite moyenne = %.1f %%",
                station, averageTemperature(), averageHumidity());
    }

    public String getStation() {
        return station;
    }

    public void setStation(String station) {
        this.station = station;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getSumTemperature() {
        return sumTemperature;
    }

    public void setSumTemperature(double sumTemperature) {
        this.sumTemperature = sumTemperature;
    }

    public double getSumHumidity() {
        return sumHumidity;
    }

    public void setSumHumidity(double sumHumidity) {
        this.sumHumidity = sumHumidity;
    }
}
