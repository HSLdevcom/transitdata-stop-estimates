package fi.hsl.transitdata.stopestimates.models;

import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class PubtransData {

    private static final Logger log = LoggerFactory.getLogger(PubtransData.class);

    public PubtransTableProtos.Common common;
    public PubtransTableProtos.DOITripInfo tripInfo;
    public InternalMessages.StopEstimate.Type eventType;

    public PubtransData(InternalMessages.StopEstimate.Type eventType, PubtransTableProtos.Common common, PubtransTableProtos.DOITripInfo tripInfo) {
        this.tripInfo = tripInfo;
        this.common = common;
        this.eventType = eventType;
    }

    public static Optional<PubtransData> parsePubtransData(TransitdataSchema schema, byte[] data) {
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

    /**
     * Throws RuntimeException if data is not valid.
     */
    public InternalMessages.StopEstimate toStopEstimate() throws Exception {
        InternalMessages.StopEstimate.Builder builder = InternalMessages.StopEstimate.newBuilder();
        builder.setSchemaVersion(builder.getSchemaVersion());

        InternalMessages.TripInfo.Builder tripBuilder = InternalMessages.TripInfo.newBuilder();
        tripBuilder.setTripId(Long.toString(tripInfo.getDvjId()));
        tripBuilder.setOperatingDay(tripInfo.getOperatingDay());
        tripBuilder.setRouteId(tripInfo.getRouteId());
        tripBuilder.setDirectionId(tripInfo.getDirectionId());//Jore format
        tripBuilder.setStartTime(tripInfo.getStartTime());
        builder.setTripInfo(tripBuilder.build());
        builder.setStopId(tripInfo.getStopId());
        builder.setStopSequence(common.getJourneyPatternSequenceNumber());

        InternalMessages.StopEstimate.Status scheduledStatus = (common.getState() == 3L) ?
                InternalMessages.StopEstimate.Status.SKIPPED :
                InternalMessages.StopEstimate.Status.SCHEDULED;

        builder.setStatus(scheduledStatus);

        builder.setType(eventType);
        builder.setEstimatedTimeUtcMs(common.getTargetUtcDateTimeMs());
        builder.setLastModifiedUtcMs(common.getLastModifiedUtcDateTimeMs());
        return builder.build();
    }

}
