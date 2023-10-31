package fi.hsl.transitdata.stopestimates;

import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import fi.hsl.transitdata.stopestimates.models.PubtransData;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class PubtransStopEstimatesFactory implements IStopEstimatesFactory {

    private static final Logger log = LoggerFactory.getLogger(PubtransStopEstimatesFactory.class);

    public Optional<List<InternalMessages.StopEstimate>> toStopEstimates(final Message message) {
        try {
            Optional<TransitdataSchema> schema = TransitdataSchema.parseFromPulsarMessage(message);
            Optional<PubtransData> maybeData = schema.flatMap(s -> parsePubtransData(s, message.getData()));

            if (maybeData.isPresent()) {
                PubtransData data = maybeData.get();

                if (validate(data)) {
                    InternalMessages.StopEstimate converted = toStopEstimate(data);
                    return Optional.of(Collections.singletonList(converted));
                }
            }
            else {
                log.warn("Received unexpected schema, ignoring.");
            }
        }
        catch (Exception e) {
            log.error("Exception while handling message", e);
        }
        return Optional.empty();
    }

    private static Optional<PubtransData> parsePubtransData(TransitdataSchema schema, byte[] data) {
        try {
            if (schema.schema == TransitdataProperties.ProtobufSchema.PubtransRoiArrival) {
                PubtransTableProtos.ROIArrival roiMessage = PubtransTableProtos.ROIArrival.parseFrom(data);
                return Optional.of(
                        new PubtransData(
                                InternalMessages.StopEstimate.Type.ARRIVAL,
                                roiMessage.getCommon(),
                                roiMessage.getTripInfo()
                        ));
            }
            else if (schema.schema == TransitdataProperties.ProtobufSchema.PubtransRoiDeparture) {
                PubtransTableProtos.ROIDeparture roiMessage = PubtransTableProtos.ROIDeparture.parseFrom(data);
                return Optional.of(
                        new PubtransData(
                                InternalMessages.StopEstimate.Type.DEPARTURE,
                                roiMessage.getCommon(),
                                roiMessage.getTripInfo()
                        ));
            }
            else {
                return Optional.empty();
            }
        }
        catch (Exception e) {
            log.error("Failed to parse PubtransData from schema " + schema.toString(), e);
            return Optional.empty();
        }
    }

    private static boolean validate(final PubtransData pubtransData) {
        return validateCommon(pubtransData.common) && validateTripInfo(pubtransData.tripInfo);
    }

    private static boolean validateCommon(PubtransTableProtos.Common common) {
        if (common == null) {
            log.error("No Common, discarding message");
            return false;
        }
        if (!common.hasIsTargetedAtJourneyPatternPointGid()) {
            log.error("No JourneyPatternPointGid, message discarded");
            return false;
        }
        if (!common.hasTargetUtcDateTimeMs()) {
            log.error("No TargetDatetime, message discarded");
            return false;
        }
        if (common.getType() == 0) {
            log.debug("Event is for a via point, message discarded");
        }
        return true;
    }

    private static boolean validateTripInfo(PubtransTableProtos.DOITripInfo tripInfo) {
        if (tripInfo == null) {
            log.error("No tripInfo, discarding message");
            return false;
        }

        if (!tripInfo.hasRouteId()) {
            log.error("TripInfo has no RouteId, discarding message");
            return false;
        }
        return true;
    }

    /**
     * Throws RuntimeException if data is not valid.
     */
    private static InternalMessages.StopEstimate toStopEstimate(final PubtransData pubtransData) throws Exception {
        InternalMessages.StopEstimate.Builder builder = InternalMessages.StopEstimate.newBuilder();
        builder.setSchemaVersion(builder.getSchemaVersion());
        InternalMessages.TripInfo.Builder tripBuilder = InternalMessages.TripInfo.newBuilder();
        tripBuilder.setTripId(Long.toString(pubtransData.tripInfo.getDvjId()));
        tripBuilder.setOperatingDay(pubtransData.tripInfo.getOperatingDay());
        tripBuilder.setRouteId(pubtransData.tripInfo.getRouteId());
        tripBuilder.setDirectionId(pubtransData.tripInfo.getDirectionId());//Jore format
        tripBuilder.setStartTime(pubtransData.tripInfo.getStartTime());
        builder.setTripInfo(tripBuilder.build());
        builder.setStopId(pubtransData.tripInfo.getStopId());
        if (pubtransData.tripInfo.hasTargetedStopId()) {
            builder.setTargetedStopId(pubtransData.tripInfo.getTargetedStopId());
        }

        builder.setStopSequence(pubtransData.common.getJourneyPatternSequenceNumber());

        InternalMessages.StopEstimate.Status scheduledStatus = (pubtransData.common.getState() == 3L) ?
                InternalMessages.StopEstimate.Status.SKIPPED :
                InternalMessages.StopEstimate.Status.SCHEDULED;

        builder.setStatus(scheduledStatus);

        builder.setType(pubtransData.eventType);
        builder.setIsViaPoint(pubtransData.common.getType() == 0);
        builder.setEstimatedTimeUtcMs(pubtransData.common.hasObservedUtcDateTimeMs() ? pubtransData.common.getObservedUtcDateTimeMs() : pubtransData.common.getTargetUtcDateTimeMs());
        builder.setObservedTime(pubtransData.common.hasObservedUtcDateTimeMs());
        builder.setLastModifiedUtcMs(pubtransData.common.getLastModifiedUtcDateTimeMs());
        return builder.build();
    }
}
