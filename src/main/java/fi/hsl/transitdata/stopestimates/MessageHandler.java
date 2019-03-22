package fi.hsl.transitdata.stopestimates;

import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataProperties.*;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;


public class MessageHandler implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageHandler.class);

    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;

    public MessageHandler(PulsarApplicationContext context) {
        consumer = context.getConsumer();
        producer = context.getProducer();
    }

    public void handleMessage(Message received) throws Exception {
        try {
            Optional<TransitdataSchema> schema = TransitdataSchema.parseFromPulsarMessage(received);
            boolean schemaOk = schema.filter(s ->
                    s.schema == ProtobufSchema.PubtransRoiDeparture ||
                    s.schema == ProtobufSchema.PubtransRoiArrival
            ).isPresent();

            if (schemaOk) {
                final long timestamp = received.getEventTime();
                byte[] data = received.getData();

                InternalMessages.StopEstimate converted = parseData(data, timestamp);
                sendPulsarMessage(received.getMessageId(), converted, timestamp, received.getKey());
            }
            else {
                log.warn("Received unexpected schema, ignoring.");
                ack(received.getMessageId()); //Ack so we don't receive it again
            }
        }
        catch (Exception e) {
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
