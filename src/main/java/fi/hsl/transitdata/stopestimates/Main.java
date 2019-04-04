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
        Optional<String> maybeSourceType = ConfigUtils.getEnv("SOURCE");

        String sourceType;
        if (maybeSourceType.isPresent()) {
            sourceType = maybeSourceType.get();
        } else {
            throw new IllegalArgumentException(String.format("Failed to get source type. Env var SOURCE may be missing."));
        }
        Config config = getConfig(sourceType);

        try (PulsarApplication app = PulsarApplication.newInstance(config)) {

            PulsarApplicationContext context = app.getContext();

            final IStopEstimatesFactory factory = getFactory(sourceType, context);
            MessageHandler router = new MessageHandler(context, factory);

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

    private static IStopEstimatesFactory getFactory(final String source, final PulsarApplicationContext context) {
        switch (source) {
            case "ptroi": return new PubtransStopEstimatesFactory();
            case "metro-schedule": return new MetroScheduleStopEstimatesFactory(context);
            default: throw new IllegalArgumentException(String.format("Failed to get IStopEstimatesFactory specified by env var SOURCE=%s.", source));
        }
    }
}
