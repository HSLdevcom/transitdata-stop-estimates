package fi.hsl.transitdata.stopestimates;

import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.MetroAtsProtos;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class MetroEstimateStopEstimatesFactory implements IStopEstimatesFactory {

    private static final Logger log = LoggerFactory.getLogger(MetroEstimateStopEstimatesFactory.class);

    public Optional<List<InternalMessages.StopEstimate>> toStopEstimates(final Message message) {
        try {
            final long timestamp = message.getEventTime();
            return Optional.of(toStopEstimates(message.getData(), timestamp));

        } catch (Exception e) {
            log.warn("Failed to produce metro estimate stop estimates.", e);
        }
        return Optional.empty();
    }

    private List<InternalMessages.StopEstimate> toStopEstimates(byte[] data, final long timestamp) throws Exception {
        MetroAtsProtos.MetroEstimate metroEstimate = MetroAtsProtos.MetroEstimate.parseFrom(data);

        return metroEstimate.getMetroRowsList().stream()
                .flatMap(metroStopEstimate -> {
                    final int stopSequence = metroEstimate.getMetroRowsList().indexOf(metroStopEstimate) + 1;
                    return toStopEstimates(metroEstimate, metroStopEstimate, stopSequence, timestamp).stream();
                })
                //TODO: if there is only a single stop estimate with SKIPPED status, assume that it is a technical problem with metro ATS and change it to SCHEDULED
                .collect(Collectors.toList());
    }

    private List<InternalMessages.StopEstimate> toStopEstimates(final MetroAtsProtos.MetroEstimate metroEstimate, final MetroAtsProtos.MetroStopEstimate metroStopEstimate, final int stopSequence, final long timestamp) {
        final Optional<InternalMessages.StopEstimate> maybeArrivalStopEstimate = toStopEstimate(metroEstimate, metroStopEstimate, stopSequence, timestamp, InternalMessages.StopEstimate.Type.ARRIVAL);
        final Optional<InternalMessages.StopEstimate> maybeDepartureStopEstimate = toStopEstimate(metroEstimate, metroStopEstimate, stopSequence, timestamp, InternalMessages.StopEstimate.Type.DEPARTURE);
        List<InternalMessages.StopEstimate> stopEstimates = new ArrayList<>();
        if (maybeArrivalStopEstimate.isPresent()) {
            stopEstimates.add(maybeArrivalStopEstimate.get());
        }
        if (maybeDepartureStopEstimate.isPresent()) {
            stopEstimates.add(maybeDepartureStopEstimate.get());
        }
        return stopEstimates;
    }

    private Optional<InternalMessages.StopEstimate> toStopEstimate(final MetroAtsProtos.MetroEstimate metroEstimate, final MetroAtsProtos.MetroStopEstimate metroStopEstimate, final int stopSequence, final long timestamp, final InternalMessages.StopEstimate.Type type) {
        InternalMessages.StopEstimate.Builder stopEstimateBuilder = InternalMessages.StopEstimate.newBuilder();
        InternalMessages.TripInfo.Builder tripBuilder = InternalMessages.TripInfo.newBuilder();

        // TripInfo
        tripBuilder.setTripId(metroEstimate.getDvjId());
        tripBuilder.setOperatingDay(metroEstimate.getOperatingDay());
        tripBuilder.setRouteId(metroEstimate.getRouteName());
        tripBuilder.setDirectionId(Integer.parseInt(metroEstimate.getDirection()));
        tripBuilder.setStartTime(metroEstimate.getStartTime());

        // StopEstimate
        if (metroEstimate.getJourneySectionprogress().equals(MetroAtsProtos.MetroProgress.CANCELLED)) {
            return Optional.empty();
        }
        stopEstimateBuilder.setSchemaVersion(stopEstimateBuilder.getSchemaVersion());
        stopEstimateBuilder.setTripInfo(tripBuilder.build());
        stopEstimateBuilder.setStopId(metroStopEstimate.getStopNumber());
        stopEstimateBuilder.setStopSequence(stopSequence);
        // Status
        Optional<InternalMessages.StopEstimate.Status> maybeStopEstimateStatus = getStopEstimateStatus(metroStopEstimate.getRowProgress());
        if (maybeStopEstimateStatus.isPresent()) {
            stopEstimateBuilder.setStatus(maybeStopEstimateStatus.get());
        } else {
            log.warn("Stop estimate had no rowProgress, stop number: {}, route name: {}, operating day: {}, start time: {}, direction: {}", metroStopEstimate.getStopNumber(), metroEstimate.getRouteName(), metroEstimate.getOperatingDay(), metroEstimate.getStartTime(), metroEstimate.getDirection());
            return Optional.empty();
        }
        stopEstimateBuilder.setType(type);
        // EstimatedTimeUtcMs & ScheduledTimeUtcMs
        boolean isForecastMissing = false;
        switch (type) {
            case ARRIVAL:
                if (!metroStopEstimate.getArrivalTimeForecast().isEmpty()) {
                    stopEstimateBuilder.setEstimatedTimeUtcMs(ZonedDateTime.parse(metroStopEstimate.getArrivalTimeForecast()).toInstant().toEpochMilli());
                    stopEstimateBuilder.setScheduledTimeUtcMs(ZonedDateTime.parse(metroStopEstimate.getArrivalTimePlanned()).toInstant().toEpochMilli());
                } else {
                    isForecastMissing = true;
                }
                break;
            case DEPARTURE:
                if (!metroStopEstimate.getDepartureTimeForecast().isEmpty()) {
                    stopEstimateBuilder.setEstimatedTimeUtcMs(ZonedDateTime.parse(metroStopEstimate.getDepartureTimeForecast()).toInstant().toEpochMilli());
                    stopEstimateBuilder.setScheduledTimeUtcMs(ZonedDateTime.parse(metroStopEstimate.getDepartureTimePlanned()).toInstant().toEpochMilli());
                } else {
                    isForecastMissing = true;
                }
                break;
            default:
                log.warn("Unrecognized type {}.", type);
                break;
        }
        if (isForecastMissing) {
            return Optional.empty();
        }

        // LastModifiedUtcMs
        stopEstimateBuilder.setLastModifiedUtcMs(timestamp);

        return Optional.of(stopEstimateBuilder.build());
    }

    private Optional<InternalMessages.StopEstimate.Status> getStopEstimateStatus(MetroAtsProtos.MetroProgress metroProgress) {
        switch (metroProgress) {
            case SCHEDULED:
            case INPROGRESS:
            case COMPLETED:
            case CANCELLED:
                return Optional.of(InternalMessages.StopEstimate.Status.SCHEDULED);
            //Do not produce SKIPPED stop estimates, as they are currently not working properly
            /*case CANCELLED:
                return Optional.of(InternalMessages.StopEstimate.Status.SKIPPED);*/
            default:
                log.warn("Unrecognized MetroProgress {}.", metroProgress);
                return Optional.empty();
        }
    }
}
