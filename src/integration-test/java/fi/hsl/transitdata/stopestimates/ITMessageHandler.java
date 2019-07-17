package fi.hsl.transitdata.stopestimates;

import fi.hsl.common.pulsar.*;

import fi.hsl.common.transitdata.MockDataUtils;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.junit.Test;

import static org.junit.Assert.*;

public class ITMessageHandler extends ITBaseTestSuite  {

    @Test
    public void testValidMessage() throws Exception {
        String testId = "-test-valid-message";
        long dvjId = MockDataUtils.generateValidJoreId();
        String route = MockDataUtils.generateValidRouteName();
        PubtransTableProtos.ROIDeparture departure = MockDataUtils.mockROIDeparture(MockDataUtils.mockCommon(dvjId).build(), MockDataUtils.mockDOITripInfo(dvjId, route));

        TestPipeline.TestLogic logic = new TestPipeline.TestLogic() {
            @Override
            public void testImpl(TestPipeline.TestContext context) throws Exception {
                final long timestamp = System.currentTimeMillis();
                sendPulsarMessage(context.source, dvjId, departure.toByteArray(), timestamp, TransitdataProperties.ProtobufSchema.PubtransRoiDeparture);
                Message<byte[]> received = TestPipeline.readOutputMessage(context);
                assertNotNull(received);
                TestPipeline.validateAcks(1, context);
            }
        };
        PulsarApplication app = createPulsarApp("integration-test.conf", testId);
        IMessageHandler pubtransMessageHandler = new MessageHandler(app.getContext(), new PubtransStopEstimatesFactory());
        testPulsarMessageHandler(pubtransMessageHandler, app, logic, testId);
        IMessageHandler metroEstimateMessageHandler = new MessageHandler(app.getContext(), new MetroEstimateStopEstimatesFactory());
        testPulsarMessageHandler(metroEstimateMessageHandler, app, logic, testId);
    }

    @Test
    public void testInvalidSchema() throws Exception {
        String testId = "-test-invalid-schema";
        long dvjId = MockDataUtils.generateValidJoreId();
        String route = MockDataUtils.generateValidRouteName();
        PubtransTableProtos.ROIDeparture departure = MockDataUtils.mockROIDeparture(MockDataUtils.mockCommon(dvjId).build(), MockDataUtils.mockDOITripInfo(dvjId, route));

        TestPipeline.TestLogic logic = new TestPipeline.TestLogic() {
            @Override
            public void testImpl(TestPipeline.TestContext context) throws Exception {
                final long timestamp = System.currentTimeMillis();
                sendPulsarMessage(context.source, dvjId, departure.toByteArray(), timestamp, TransitdataProperties.ProtobufSchema.InternalMessagesTripCancellation);
                Message<byte[]> received = TestPipeline.readOutputMessage(context);
                assertNull(received);
                TestPipeline.validateAcks(1, context);
            }
        };
        PulsarApplication app = createPulsarApp("integration-test.conf", testId);
        IMessageHandler pubtransMessageHandler = new MessageHandler(app.getContext(), new PubtransStopEstimatesFactory());
        testPulsarMessageHandler(pubtransMessageHandler, app, logic, testId);
        IMessageHandler metroEstimateMessageHandler = new MessageHandler(app.getContext(), new MetroEstimateStopEstimatesFactory());
        testPulsarMessageHandler(metroEstimateMessageHandler, app, logic, testId);
    }

    @Test
    public void testInvalidMessage() throws Exception {
        String testId = "-test-invalid-message";
        long dvjId = MockDataUtils.generateValidJoreId();
        String route = MockDataUtils.generateValidRouteName();
        PubtransTableProtos.Common.Builder builder = MockDataUtils.mockCommon(dvjId);
        builder.setType(0);
        PubtransTableProtos.ROIDeparture departure = MockDataUtils.mockROIDeparture(builder.build(), MockDataUtils.mockDOITripInfo(dvjId, route));

        TestPipeline.TestLogic logic = new TestPipeline.TestLogic() {
            @Override
            public void testImpl(TestPipeline.TestContext context) throws Exception {
                final long timestamp = System.currentTimeMillis();
                sendPulsarMessage(context.source, dvjId, departure.toByteArray(), timestamp, TransitdataProperties.ProtobufSchema.PubtransRoiArrival);
                Message<byte[]> received = TestPipeline.readOutputMessage(context);
                assertNull(received);
                TestPipeline.validateAcks(1, context);
            }
        };
        PulsarApplication app = createPulsarApp("integration-test.conf", testId);
        IMessageHandler pubtransMessageHandler = new MessageHandler(app.getContext(), new PubtransStopEstimatesFactory());
        testPulsarMessageHandler(pubtransMessageHandler, app, logic, testId);
        IMessageHandler metroEstimateMessageHandler = new MessageHandler(app.getContext(), new MetroEstimateStopEstimatesFactory());
        testPulsarMessageHandler(metroEstimateMessageHandler, app, logic, testId);
    }

    static void sendPulsarMessage(Producer<byte[]> producer, long dvjId, byte[] payload, long timestampEpochMs, TransitdataProperties.ProtobufSchema schema) throws Exception {
        producer.newMessage().value(payload)
                .eventTime(timestampEpochMs)
                .key(Long.toString(dvjId))
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, schema.toString())
                .send();
    }
}
