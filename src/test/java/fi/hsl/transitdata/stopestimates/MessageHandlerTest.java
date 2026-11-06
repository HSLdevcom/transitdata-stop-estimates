package fi.hsl.transitdata.stopestimates;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MessageHandlerTest {

    @Test
    @SuppressWarnings("unchecked")
    public void forwardsSourceTimestampFromIncomingMessage() throws Exception {
        final Consumer<byte[]> consumer = mock(Consumer.class);
        final Producer<byte[]> producer = mock(Producer.class);
        final TypedMessageBuilder<byte[]> builder = mock(TypedMessageBuilder.class, RETURNS_SELF);
        when(producer.newMessage()).thenReturn(builder);
        when(builder.sendAsync()).thenReturn(CompletableFuture.completedFuture(mock(MessageId.class)));
        when(consumer.acknowledgeAsync(any(MessageId.class))).thenReturn(CompletableFuture.completedFuture(null));

        final PulsarApplicationContext context = mock(PulsarApplicationContext.class);
        when(context.getConsumer()).thenReturn(consumer);
        when(context.getProducer()).thenReturn(producer);

        final Message received = mock(Message.class);
        when(received.getMessageId()).thenReturn(mock(MessageId.class));
        when(received.getEventTime()).thenReturn(5000L);
        when(received.getKey()).thenReturn("key");
        when(received.getProperty(TransitdataProperties.KEY_SOURCE_MESSAGE_TIMESTAMP_MS)).thenReturn("1234");

        final InternalMessages.TripInfo tripInfo = mock(InternalMessages.TripInfo.class);
        when(tripInfo.getTripId()).thenReturn("trip-1");
        final InternalMessages.StopEstimate estimate = mock(InternalMessages.StopEstimate.class);
        when(estimate.getSchemaVersion()).thenReturn(1);
        when(estimate.getTripInfo()).thenReturn(tripInfo);
        when(estimate.toByteArray()).thenReturn(new byte[0]);
        final IStopEstimatesFactory factory = mock(IStopEstimatesFactory.class);
        when(factory.toStopEstimates(received)).thenReturn(Optional.of(List.of(estimate)));

        new MessageHandler(context, factory).handleMessage(received);

        verify(builder).property(TransitdataProperties.KEY_SOURCE_MESSAGE_TIMESTAMP_MS, "1234");
    }
}
