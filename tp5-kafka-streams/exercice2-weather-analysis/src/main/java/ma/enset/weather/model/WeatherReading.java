package ma.enset.weather.model;

/**
 * Représente une mesure météo unique, déjà parsée et validée.
 */
public class WeatherReading {

    private String station;
    private double temperature;
    private double humidity;

    public WeatherReading() {
    }

    public WeatherReading(String station, double temperature, double humidity) {
        this.station = station;
        this.temperature = temperature;
        this.humidity = humidity;
    }

    /**
     * Parse une ligne CSV "station,temperature,humidity".
     * Retourne null si la ligne est mal formée (pour être filtrée en aval sans faire
     * planter le stream thread).
     */
    public static WeatherReading parse(String csvLine) {
        if (csvLine == null || csvLine.isBlank()) {
            return null;
        }
        String[] parts = csvLine.trim().split(",");
        if (parts.length != 3) {
            return null;
        }
        try {
            String station = parts[0].trim();
            double temperature = Double.parseDouble(parts[1].trim());
            double humidity = Double.parseDouble(parts[2].trim());
            return new WeatherReading(station, temperature, humidity);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public WeatherReading toFahrenheit() {
        double fahrenheit = (this.temperature * 9.0 / 5.0) + 32;
        return new WeatherReading(this.station, fahrenheit, this.humidity);
    }

    public String getStation() {
        return station;
    }

    public void setStation(String station) {
        this.station = station;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public double getHumidity() {
        return humidity;
    }

    public void setHumidity(double humidity) {
        this.humidity = humidity;
    }

    @Override
    public String toString() {
        return station + "," + temperature + "," + humidity;
    }
}
