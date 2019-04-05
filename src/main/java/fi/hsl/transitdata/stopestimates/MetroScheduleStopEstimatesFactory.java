package fi.hsl.transitdata.stopestimates;

import com.fasterxml.jackson.databind.ObjectMapper;
import fi.hsl.common.metro.MetroStops;
import fi.hsl.common.mqtt.proto.Mqtt;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.stopestimates.models.MetroSchedule;
import fi.hsl.transitdata.stopestimates.models.MetroScheduleRouteRow;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Optional;
import java.util.stream.Collectors;

public class MetroScheduleStopEstimatesFactory implements IStopEstimatesFactory {

    private static final Logger log = LoggerFactory.getLogger(MetroScheduleStopEstimatesFactory.class);

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Pattern pattern = Pattern.compile("^metro-mipro-ats\\/v1\\/schedule\\/(.+)\\/(.+)$");

    private Jedis jedis;

    public MetroScheduleStopEstimatesFactory(final PulsarApplicationContext context) {
        this.jedis = context.getJedis();
    }

    public Optional<List<InternalMessages.StopEstimate>> toStopEstimates(final Message message) {
        try {
            Optional<TransitdataSchema> maybeSchema = TransitdataSchema.parseFromPulsarMessage(message);
            if (maybeSchema.isPresent()) {
                final byte[] data = message.getData();
                final Mqtt.RawMessage mqttMessage = Mqtt.RawMessage.parseFrom(data);
                final String topic = mqttMessage.getTopic();
                final byte[] payload = mqttMessage.getPayload().toByteArray();
                Optional<MetroScheduleTopicParameters> maybeTopicParams = parseTopic(topic);
                Optional<MetroSchedule> maybeSchedule = parsePayload(payload);

                if (maybeTopicParams.isPresent() && maybeSchedule.isPresent()) {
                    final MetroScheduleTopicParameters topicParams = maybeTopicParams.get();
                    final MetroSchedule schedule = maybeSchedule.get();
                    final long lastModifiedUtcMs = mqttMessage.getLastModifiedUtcDateTimeMs();
                    List<InternalMessages.StopEstimate> stopEstimates = toStopEstimates(topicParams, schedule, lastModifiedUtcMs);
                    return Optional.of(stopEstimates);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to produce metro schedule stop estimates.", e);
        }
        return Optional.empty();
    }

    private List<InternalMessages.StopEstimate> toStopEstimates(final MetroScheduleTopicParameters topicParams, final MetroSchedule schedule, final long lastModifiedUtcMs) throws Exception {
        final String[] shortNames = schedule.routeName.split("-");
        if (shortNames.length != 2) {
            throw new IllegalArgumentException(String.format("Failed to parse metro schedule route name %s.", schedule.routeName));
        }
        final String startStopShortName = shortNames[0];
        final String endStopShortName = shortNames[1];
        final Optional<Integer> maybeJoreDirection = MetroStops.getJoreDirection(startStopShortName, endStopShortName);
        if (!maybeJoreDirection.isPresent()) {
            throw new IllegalArgumentException(String.format("Failed to get jore direction from metro stops %s and %s", startStopShortName, endStopShortName));
        }
        final int joreDirection = maybeJoreDirection.get();
        final String startDatetime = schedule.beginTime;
        final String metroId = TransitdataProperties.formatMetroId(startStopShortName, startDatetime);
        return schedule.routeRows.stream()
                .flatMap(routeRow -> {
                    final int stopSequence = schedule.routeRows.indexOf(routeRow) + 1;
                    if (stopSequence == -1) {
                        // TODO: implement routeRow.toString()?
                        throw new IllegalArgumentException(String.format("Failed to find stop sequence for metro schedule route row %s.", routeRow));
                    }
                    return toStopEstimates(routeRow, stopSequence, metroId, lastModifiedUtcMs, joreDirection).stream();
                })
                .collect(Collectors.toList());
    }

    private List<InternalMessages.StopEstimate> toStopEstimates(final MetroScheduleRouteRow routeRow, final int stopSequence, final String metroId, final long lastModifiedUtcMs, final int joreDirection) {
        final InternalMessages.StopEstimate arrivalStopEstimate = toStopEstimate(routeRow, stopSequence, metroId, lastModifiedUtcMs, InternalMessages.StopEstimate.Type.ARRIVAL, joreDirection);
        final InternalMessages.StopEstimate departureStopEstimate = toStopEstimate(routeRow, stopSequence, metroId, lastModifiedUtcMs, InternalMessages.StopEstimate.Type.DEPARTURE, joreDirection);
        return Arrays.asList(arrivalStopEstimate, departureStopEstimate);
    }

    private InternalMessages.StopEstimate toStopEstimate(final MetroScheduleRouteRow routeRow, final int stopSequence, final String metroId, final long lastModifiedUtcMs, final InternalMessages.StopEstimate.Type type, final int joreDirection) {
        InternalMessages.StopEstimate.Builder builder = InternalMessages.StopEstimate.newBuilder();
        builder.setSchemaVersion(builder.getSchemaVersion());

        // TripInfo
        InternalMessages.TripInfo.Builder tripBuilder = InternalMessages.TripInfo.newBuilder();
        Optional<Map<String, String>> metroJourneyData = getMetroJourneyData(metroId);
        metroJourneyData.ifPresent(map -> {
            if (map.containsKey(TransitdataProperties.KEY_DVJ_ID))
                tripBuilder.setTripId(map.get(TransitdataProperties.KEY_DVJ_ID));
            if (map.containsKey(TransitdataProperties.KEY_OPERATING_DAY))
                tripBuilder.setOperatingDay(map.get(TransitdataProperties.KEY_OPERATING_DAY));
            if (map.containsKey(TransitdataProperties.KEY_ROUTE_NAME))
                tripBuilder.setRouteId(map.get(TransitdataProperties.KEY_ROUTE_NAME));
            if (map.containsKey(TransitdataProperties.KEY_DIRECTION))
                tripBuilder.setDirectionId(Integer.parseInt(map.get(TransitdataProperties.KEY_DIRECTION)));
            if (map.containsKey(TransitdataProperties.KEY_START_TIME))
                tripBuilder.setStartTime(map.get(TransitdataProperties.KEY_START_TIME));
        });
        builder.setTripInfo(tripBuilder.build());

        // StopID
        final Optional<String> maybeStopNumber = MetroStops.getStopNumber(routeRow.station, joreDirection);
        if (maybeStopNumber.isPresent()) {
            builder.setStopId(maybeStopNumber.get());
        }

        // StopSequence
        builder.setStopSequence(stopSequence);

        // Status
        switch (routeRow.rowProgress) {
            case SCHEDULED:
            case INPROGRESS:
            case COMPLETED:
                builder.setStatus(InternalMessages.StopEstimate.Status.SCHEDULED);
                break;
            case CANCELLED:
                builder.setStatus(InternalMessages.StopEstimate.Status.SKIPPED);
                break;
            default:
                log.warn("Unrecognized status {}.", routeRow.rowProgress);
                break;
        }

        // Type
        builder.setType(type);

        // EstimatedTimeUtcMs
        switch (type) {
            case ARRIVAL:
                builder.setEstimatedTimeUtcMs(ZonedDateTime.parse(routeRow.arrivalTimeForecast).toInstant().toEpochMilli());
                builder.setScheduledTimeUtcMs(ZonedDateTime.parse(routeRow.arrivalTimePlanned).toInstant().toEpochMilli());
                break;
            case DEPARTURE:
                builder.setEstimatedTimeUtcMs(ZonedDateTime.parse(routeRow.departureTimeForecast).toInstant().toEpochMilli());
                builder.setScheduledTimeUtcMs(ZonedDateTime.parse(routeRow.departureTimePlanned).toInstant().toEpochMilli());
                break;
            default:
                log.warn("Unrecognized type {}.", type);
                break;
        }

        // LastModifiedUtcMs
        builder.setLastModifiedUtcMs(lastModifiedUtcMs);

        return builder.build();
    }

    private Optional<Map<String, String>> getMetroJourneyData(final String metroId) {
        return Optional.ofNullable(jedis.hgetAll(metroId));
    }

    private Optional<MetroSchedule> parsePayload(final byte[] payload) {
        try {
            MetroSchedule schedule = mapper.readValue(payload, MetroSchedule.class);
            return Optional.of(schedule);
        } catch (Exception e) {
            log.warn(String.format("Failed to parse payload %s.", new String(payload)), e);
        }
        return Optional.empty();
    }

    private Optional<MetroScheduleTopicParameters> parseTopic(final String topic) {
        Matcher matcher = pattern.matcher(topic);
        if (matcher.matches()) {
            return Optional.of(new MetroScheduleTopicParameters(matcher.group(1), matcher.group(2)));
        } else {
            log.warn("Failed to parse topic {}.", topic);
        }
        return Optional.empty();
    }

    private class MetroScheduleTopicParameters {
        public String shiftNumber;
        public String jsId;

        public MetroScheduleTopicParameters(final String shiftNumber, final String jsId) {
            this.shiftNumber = shiftNumber;
            this.jsId = jsId;
        }
    }
}
