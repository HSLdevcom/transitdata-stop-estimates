package fi.hsl.transitdata.stopestimates;

import com.typesafe.config.Config;
import java.util.Optional;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.pulsar.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Starting Stop Estimate Parser");
        Optional<String> sourceType = ConfigUtils.getEnv("SOURCE");
        Config config = null;
        if (sourceType.isPresent()) {
            config = getConfig(sourceType.get());
        } else {
            config = ConfigParser.createConfig();
        }

        try (PulsarApplication app = PulsarApplication.newInstance(config)) {

            PulsarApplicationContext context = app.getContext();

            MessageHandler router = new MessageHandler(context);

            log.info("Start handling the messages");
            app.launchWithHandler(router);
        } catch (Exception e) {
            log.error("Exception at main", e);
        }
    }

    private static Config getConfig(final String source) {
        switch (source) {
            case "ptroi": return ConfigParser.createConfig("ptroi.conf");
            case "metro-schedule": return ConfigParser.createConfig("metro-schedule.conf");
            default: throw new IllegalArgumentException(String.format("Failed to get Config specified by env var SOURCE=%s.", source));
        }
    }
}
