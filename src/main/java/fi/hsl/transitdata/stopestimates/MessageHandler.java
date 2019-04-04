package fi.hsl.transitdata.stopestimates;

import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataProperties.*;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;


public class MessageHandler implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageHandler.class);

    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;
    private IStopEstimatesFactory factory;

    public MessageHandler(PulsarApplicationContext context, final IStopEstimatesFactory factory) {
        consumer = context.getConsumer();
        producer = context.getProducer();
        this.factory = factory;
    }

    public void handleMessage(Message received) throws Exception {
        try {
            final Optional<List<InternalMessages.StopEstimate>> maybeStopEstimates = factory.toStopEstimates(received);

            if (maybeStopEstimates.isPresent()) {
                final long timestamp = received.getEventTime();
                final List<InternalMessages.StopEstimate> stopEstimates = maybeStopEstimates.get();
                stopEstimates.forEach(stopEstimate -> {
                    sendPulsarMessage(received.getMessageId(), stopEstimate, timestamp, received.getKey());
                });
            } else {
                log.warn("Received unexpected schema, ignoring.");
                ack(received.getMessageId()); //Ack so we don't receive it again
            }
        } catch (Exception e) {
            log.error("Exception while handling message", e);
        }
    }

    private void ack(MessageId received) {
        consumer.acknowledgeAsync(received)
                .exceptionally(throwable -> {
                    log.error("Failed to ack Pulsar message", throwable);
                    return null;
                })
                .thenRun(() -> {});
    }

    InternalMessages.StopEstimate parseData(byte[] data, long timestamp) throws Exception {
        //TODO
        return null;
    }

    private void sendPulsarMessage(MessageId received, InternalMessages.StopEstimate estimate, long timestamp, String key) {

        producer.newMessage()
                .key(key)
                .eventTime(timestamp)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, ProtobufSchema.InternalMessagesStopEstimate.toString())
                .property(TransitdataProperties.KEY_SCHEMA_VERSION, Integer.toString(estimate.getSchemaVersion()))
                .property(TransitdataProperties.KEY_DVJ_ID, estimate.getTripInfo().getTripId()) // TODO remove once TripUpdateProcessor won't need it anymore
                .value(estimate.toByteArray())
                .sendAsync()
                .whenComplete((MessageId id, Throwable t) -> {
                    if (t != null) {
                        log.error("Failed to send Pulsar message", t);
                        //Should we abort?
                    }
                    else {
                        //Does this become a bottleneck? Does pulsar send more messages before we ack the previous one?
                        //If yes we need to get rid of this
                        ack(received);
                    }
                });

    }
}
