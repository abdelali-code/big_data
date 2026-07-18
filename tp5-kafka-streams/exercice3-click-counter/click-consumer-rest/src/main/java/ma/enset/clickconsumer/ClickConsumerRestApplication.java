package ma.enset.clickconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class ClickConsumerRestApplication {

    public static void main(String[] args) {
        SpringApplication.run(ClickConsumerRestApplication.class, args);
    }
}
