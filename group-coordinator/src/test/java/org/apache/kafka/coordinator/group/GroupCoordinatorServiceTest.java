/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group;

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidFetchSizeException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.server.share.persister.DefaultStatePersister;
import org.apache.kafka.server.share.persister.DeleteShareGroupStateParameters;
import org.apache.kafka.server.share.persister.DeleteShareGroupStateResult;
import org.apache.kafka.server.share.persister.GroupTopicPartitionData;
import org.apache.kafka.server.share.persister.NoOpStatePersister;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.PartitionIdData;
import org.apache.kafka.server.share.persister.Persister;
import org.apache.kafka.server.share.persister.ReadShareGroupStateSummaryParameters;
import org.apache.kafka.server.share.persister.ReadShareGroupStateSummaryResult;
import org.apache.kafka.server.share.persister.TopicData;
import org.apache.kafka.server.util.FutureUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.coordinator.common.runtime.TestUtil.requestContext;
import static org.apache.kafka.coordinator.group.GroupConfigManagerTest.createConfigManager;
import static org.apache.kafka.server.share.persister.DeleteShareGroupStateParameters.EMPTY_PARAMS;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GroupCoordinatorServiceTest {

    private static final String TOPIC_NAME = "test-topic";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();

    private static Stream<Arguments> testGroupHeartbeatWithExceptionSource() {
        return Stream.of(
            Arguments.arguments(new UnknownTopicOrPartitionException(), Errors.COORDINATOR_NOT_AVAILABLE.code(), null),
            Arguments.arguments(new NotEnoughReplicasException(), Errors.COORDINATOR_NOT_AVAILABLE.code(), null),
            Arguments.arguments(new org.apache.kafka.common.errors.TimeoutException(), Errors.COORDINATOR_NOT_AVAILABLE.code(), null),
            Arguments.arguments(new NotLeaderOrFollowerException(), Errors.NOT_COORDINATOR.code(), null),
            Arguments.arguments(new KafkaStorageException(), Errors.NOT_COORDINATOR.code(), null),
            Arguments.arguments(new RecordTooLargeException(), Errors.UNKNOWN_SERVER_ERROR.code(), null),
            Arguments.arguments(new RecordBatchTooLargeException(), Errors.UNKNOWN_SERVER_ERROR.code(), null),
            Arguments.arguments(new InvalidFetchSizeException(""), Errors.UNKNOWN_SERVER_ERROR.code(), null),
            Arguments.arguments(new InvalidRequestException("Invalid"), Errors.INVALID_REQUEST.code(), "Invalid")
        );
    }

    @SuppressWarnings("unchecked")
    private CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> mockRuntime() {
        return (CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord>) mock(CoordinatorRuntime.class);
    }

    private GroupCoordinatorConfig createConfig() {
        return GroupCoordinatorConfigTest.createGroupCoordinatorConfig(4096, 600000L, 24);
    }

    @Test
    public void testStartupShutdown() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setRuntime(runtime)
            .setConfig(createConfig())
            .build(true);

        service.shutdown();

        verify(runtime, times(1)).close();
    }

    @Test
    public void testConsumerGroupHeartbeatWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        ConsumerGroupHeartbeatRequestData request = new ConsumerGroupHeartbeatRequestData()
            .setGroupId("foo");

        CompletableFuture<ConsumerGroupHeartbeatResponseData> future = service.consumerGroupHeartbeat(
            requestContext(ApiKeys.CONSUMER_GROUP_HEARTBEAT),
            request
        );

        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
            future.get()
        );
    }

    @Test
    public void testConsumerGroupHeartbeat() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setRuntime(runtime)
            .setConfig(createConfig())
            .build(true);

        ConsumerGroupHeartbeatRequestData request = new ConsumerGroupHeartbeatRequestData()
            .setGroupId("foo");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("consumer-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            new ConsumerGroupHeartbeatResponseData()
        ));

        CompletableFuture<ConsumerGroupHeartbeatResponseData> future = service.consumerGroupHeartbeat(
            requestContext(ApiKeys.CONSUMER_GROUP_HEARTBEAT),
            request
        );

        assertEquals(new ConsumerGroupHeartbeatResponseData(), future.get(5, TimeUnit.SECONDS));
    }

    @ParameterizedTest
    @MethodSource("testGroupHeartbeatWithExceptionSource")
    public void testConsumerGroupHeartbeatWithException(
        Throwable exception,
        short expectedErrorCode,
        String expectedErrorMessage
    ) throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        ConsumerGroupHeartbeatRequestData request = new ConsumerGroupHeartbeatRequestData()
            .setGroupId("foo");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("consumer-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(exception));

        CompletableFuture<ConsumerGroupHeartbeatResponseData> future = service.consumerGroupHeartbeat(
            requestContext(ApiKeys.CONSUMER_GROUP_HEARTBEAT),
            request
        );

        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(expectedErrorCode)
                .setErrorMessage(expectedErrorMessage),
            future.get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testPartitionFor() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        assertThrows(CoordinatorNotAvailableException.class,
            () -> service.partitionFor("foo"));

        service.startup(() -> 10);

        assertEquals(Utils.abs("foo".hashCode()) % 10, service.partitionFor("foo"));
    }

    @Test
    public void testGroupMetadataTopicConfigs() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        Properties expectedProperties = new Properties();
        expectedProperties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        expectedProperties.put(TopicConfig.COMPRESSION_TYPE_CONFIG, BrokerCompressionType.PRODUCER.name);
        expectedProperties.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1000");

        assertEquals(expectedProperties, service.groupMetadataTopicConfigs());
    }

    @Test
    public void testOnElection() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        assertThrows(CoordinatorNotAvailableException.class,
            () -> service.onElection(5, 10));

        service.startup(() -> 1);
        service.onElection(5, 10);

        verify(runtime, times(1)).scheduleLoadOperation(
            new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 5),
            10
        );
    }

    @Test
    public void testOnResignation() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        assertThrows(CoordinatorNotAvailableException.class,
            () -> service.onResignation(5, OptionalInt.of(10)));

        service.startup(() -> 1);
        service.onResignation(5, OptionalInt.of(10));

        verify(runtime, times(1)).scheduleUnloadOperation(
            new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 5),
            OptionalInt.of(10)
        );
    }

    @Test
    public void testOnResignationWithEmptyLeaderEpoch() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        service.onResignation(5, OptionalInt.empty());

        verify(runtime, times(1)).scheduleUnloadOperation(
            new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 5),
            OptionalInt.empty()
        );
    }

    @Test
    public void testJoinGroup() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        JoinGroupRequestData request = new JoinGroupRequestData()
            .setGroupId("foo")
            .setSessionTimeoutMs(1000);

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-join"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            new JoinGroupResponseData()
        ));

        CompletableFuture<JoinGroupResponseData> responseFuture = service.joinGroup(
            requestContext(ApiKeys.JOIN_GROUP),
            request,
            BufferSupplier.NO_CACHING
        );

        assertFalse(responseFuture.isDone());
    }

    @Test
    public void testJoinGroupWithException() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        JoinGroupRequestData request = new JoinGroupRequestData()
            .setGroupId("foo")
            .setSessionTimeoutMs(1000);

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-join"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(new IllegalStateException()));

        CompletableFuture<JoinGroupResponseData> future = service.joinGroup(
            requestContext(ApiKeys.JOIN_GROUP),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(
            new JoinGroupResponseData()
                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code()),
            future.get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testJoinGroupInvalidGroupId() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        JoinGroupRequestData request = new JoinGroupRequestData()
            .setGroupId(null)
            .setMemberId(UNKNOWN_MEMBER_ID);

        RequestContext context = new RequestContext(
            new RequestHeader(
                ApiKeys.JOIN_GROUP,
                ApiKeys.JOIN_GROUP.latestVersion(),
                "client",
                0
            ),
            "1",
            InetAddress.getLoopbackAddress(),
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            ClientInformation.EMPTY,
            false
        );

        CompletableFuture<JoinGroupResponseData> response = service.joinGroup(
            context,
            request,
            BufferSupplier.NO_CACHING
        );

        assertTrue(response.isDone());
        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.INVALID_GROUP_ID.code())
            .setMemberId(UNKNOWN_MEMBER_ID);

        assertEquals(expectedResponse, response.get());
    }

    @Test
    public void testJoinGroupWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        JoinGroupRequestData request = new JoinGroupRequestData()
            .setGroupId("foo");

        CompletableFuture<JoinGroupResponseData> future = service.joinGroup(
            requestContext(ApiKeys.JOIN_GROUP),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(
            new JoinGroupResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
            future.get()
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {120 - 1, 10 * 5 * 1000 + 1})
    public void testJoinGroupInvalidSessionTimeout(int sessionTimeoutMs) throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withSessionTimeoutMs(sessionTimeoutMs)
            .build();

        CompletableFuture<JoinGroupResponseData> future = service.joinGroup(
            requestContext(ApiKeys.JOIN_GROUP),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(
            new JoinGroupResponseData()
                .setErrorCode(Errors.INVALID_SESSION_TIMEOUT.code()),
            future.get()
        );
    }

    @Test
    public void testSyncGroup() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        SyncGroupRequestData request = new SyncGroupRequestData()
            .setGroupId("foo");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-sync"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            new SyncGroupResponseData()
        ));

        CompletableFuture<SyncGroupResponseData> responseFuture = service.syncGroup(
            requestContext(ApiKeys.SYNC_GROUP),
            request,
            BufferSupplier.NO_CACHING
        );

        assertFalse(responseFuture.isDone());
    }

    @Test
    public void testSyncGroupWithException() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        SyncGroupRequestData request = new SyncGroupRequestData()
            .setGroupId("foo");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-sync"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(new IllegalStateException()));

        CompletableFuture<SyncGroupResponseData> future = service.syncGroup(
            requestContext(ApiKeys.SYNC_GROUP),
            request,
            BufferSupplier.NO_CACHING
        );

        assertTrue(future.isDone());
        assertEquals(
            new SyncGroupResponseData()
                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code()),
            future.get()
        );
    }

    @Test
    public void testSyncGroupInvalidGroupId() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        SyncGroupRequestData request = new SyncGroupRequestData()
            .setGroupId(null)
            .setMemberId(UNKNOWN_MEMBER_ID);

        CompletableFuture<SyncGroupResponseData> response = service.syncGroup(
            requestContext(ApiKeys.SYNC_GROUP),
            request,
            BufferSupplier.NO_CACHING
        );

        assertTrue(response.isDone());
        SyncGroupResponseData expectedResponse = new SyncGroupResponseData()
            .setErrorCode(Errors.INVALID_GROUP_ID.code());

        assertEquals(expectedResponse, response.get());
    }

    @Test
    public void testSyncGroupWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        SyncGroupRequestData request = new SyncGroupRequestData()
            .setGroupId("foo");

        CompletableFuture<SyncGroupResponseData> future = service.syncGroup(
            requestContext(ApiKeys.SYNC_GROUP),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(
            new SyncGroupResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
            future.get()
        );
    }

    @Test
    public void testHeartbeat() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        HeartbeatRequestData request = new HeartbeatRequestData()
            .setGroupId("foo");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            new HeartbeatResponseData()
        ));

        CompletableFuture<HeartbeatResponseData> future = service.heartbeat(
            requestContext(ApiKeys.HEARTBEAT),
            request
        );

        assertTrue(future.isDone());
        assertEquals(new HeartbeatResponseData(), future.get());
    }

    @Test
    public void testHeartbeatCoordinatorNotAvailableException() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        HeartbeatRequestData request = new HeartbeatRequestData()
            .setGroupId("foo");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(
            new CoordinatorLoadInProgressException(null)
        ));

        CompletableFuture<HeartbeatResponseData> future = service.heartbeat(
            requestContext(ApiKeys.HEARTBEAT),
            request
        );

        assertTrue(future.isDone());
        assertEquals(new HeartbeatResponseData(), future.get());
    }

    @Test
    public void testHeartbeatCoordinatorException() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        HeartbeatRequestData request = new HeartbeatRequestData()
            .setGroupId("foo");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(
            new RebalanceInProgressException()
        ));

        CompletableFuture<HeartbeatResponseData> future = service.heartbeat(
            requestContext(ApiKeys.HEARTBEAT),
            request
        );

        assertTrue(future.isDone());
        assertEquals(
            new HeartbeatResponseData().setErrorCode(Errors.REBALANCE_IN_PROGRESS.code()),
            future.get()
        );
    }

    @Test
    public void testHeartbeatWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        HeartbeatRequestData request = new HeartbeatRequestData()
            .setGroupId("foo");

        CompletableFuture<HeartbeatResponseData> future = service.heartbeat(
            requestContext(ApiKeys.CONSUMER_GROUP_HEARTBEAT),
            request
        );

        assertEquals(
            new HeartbeatResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
            future.get()
        );
    }

    @Test
    public void testListGroups() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        service.startup(() -> 3);

        List<ListGroupsResponseData.ListedGroup> expectedResults = Arrays.asList(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId("group0")
                .setProtocolType("protocol1")
                .setGroupState("Stable")
                .setGroupType("classic"),
            new ListGroupsResponseData.ListedGroup()
                .setGroupId("group1")
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setGroupState("Empty")
                .setGroupType("consumer"),
            new ListGroupsResponseData.ListedGroup()
                .setGroupId("group2")
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setGroupState("Dead")
                .setGroupType("consumer")
        );

        when(runtime.scheduleReadAllOperation(
            ArgumentMatchers.eq("list-groups"),
            ArgumentMatchers.any()
        )).thenReturn(Arrays.asList(
            CompletableFuture.completedFuture(Collections.singletonList(expectedResults.get(0))),
            CompletableFuture.completedFuture(Collections.singletonList(expectedResults.get(1))),
            CompletableFuture.completedFuture(Collections.singletonList(expectedResults.get(2)))
        ));

        CompletableFuture<ListGroupsResponseData> responseFuture = service.listGroups(
            requestContext(ApiKeys.LIST_GROUPS),
            new ListGroupsRequestData()
        );

        assertEquals(expectedResults, responseFuture.get(5, TimeUnit.SECONDS).groups());
    }

    @Test
    public void testListGroupsFailedWithNotCoordinatorException()
        throws InterruptedException, ExecutionException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        service.startup(() -> 3);

        List<ListGroupsResponseData.ListedGroup> expectedResults = Arrays.asList(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId("group0")
                .setProtocolType("protocol1")
                .setGroupState("Stable")
                .setGroupType("classic"),
            new ListGroupsResponseData.ListedGroup()
                .setGroupId("group1")
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setGroupState("Empty")
                .setGroupType("consumer")
        );

        when(runtime.scheduleReadAllOperation(
            ArgumentMatchers.eq("list-groups"),
            ArgumentMatchers.any()
        )).thenReturn(Arrays.asList(
            CompletableFuture.completedFuture(Collections.singletonList(expectedResults.get(0))),
            CompletableFuture.completedFuture(Collections.singletonList(expectedResults.get(1))),
            FutureUtils.failedFuture(new NotCoordinatorException(""))
        ));

        CompletableFuture<ListGroupsResponseData> responseFuture = service.listGroups(
            requestContext(ApiKeys.LIST_GROUPS),
            new ListGroupsRequestData()
        );

        assertEquals(expectedResults, responseFuture.get(5, TimeUnit.SECONDS).groups());
    }

    @Test
    public void testListGroupsWithFailure()
        throws InterruptedException, ExecutionException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        service.startup(() -> 3);

        when(runtime.scheduleReadAllOperation(
            ArgumentMatchers.eq("list-groups"),
            ArgumentMatchers.any()
        )).thenReturn(Arrays.asList(
            CompletableFuture.completedFuture(Collections.emptyList()),
            CompletableFuture.completedFuture(Collections.emptyList()),
            FutureUtils.failedFuture(new CoordinatorLoadInProgressException(""))
        ));

        CompletableFuture<ListGroupsResponseData> responseFuture = service.listGroups(
            requestContext(ApiKeys.LIST_GROUPS),
            new ListGroupsRequestData()
        );

        assertEquals(
            new ListGroupsResponseData()
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code()),
            responseFuture.get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testListGroupsWithEmptyTopicPartitions() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 0;
        service.startup(() -> partitionCount);

        ListGroupsRequestData request = new ListGroupsRequestData();

        CompletableFuture<ListGroupsResponseData> future = service.listGroups(
            requestContext(ApiKeys.LIST_GROUPS),
            request
        );

        assertEquals(
            new ListGroupsResponseData(),
            future.get()
        );
    }

    @Test
    public void testListGroupsWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        ListGroupsRequestData request = new ListGroupsRequestData();

        CompletableFuture<ListGroupsResponseData> future = service.listGroups(
            requestContext(ApiKeys.LIST_GROUPS),
            request
        );

        assertEquals(
            new ListGroupsResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
            future.get()
        );
    }

    @Test
    public void testDescribeGroups() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build();
        int partitionCount = 2;
        service.startup(() -> partitionCount);

        DescribeGroupsResponseData.DescribedGroup describedGroup1 = new DescribeGroupsResponseData.DescribedGroup()
            .setGroupId("group-id-1");
        DescribeGroupsResponseData.DescribedGroup describedGroup2 = new DescribeGroupsResponseData.DescribedGroup()
            .setGroupId("group-id-2");
        List<DescribeGroupsResponseData.DescribedGroup> expectedDescribedGroups = Arrays.asList(
            describedGroup1,
            describedGroup2
        );

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("describe-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(describedGroup1)));

        CompletableFuture<Object> describedGroupFuture = new CompletableFuture<>();
        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("describe-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)),
            ArgumentMatchers.any()
        )).thenReturn(describedGroupFuture);

        CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>> future =
            service.describeGroups(requestContext(ApiKeys.DESCRIBE_GROUPS), Arrays.asList("group-id-1", "group-id-2"));

        assertFalse(future.isDone());
        describedGroupFuture.complete(Collections.singletonList(describedGroup2));
        assertEquals(expectedDescribedGroups, future.get());
    }

    @Test
    public void testDescribeGroupsInvalidGroupId() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build();
        int partitionCount = 1;
        service.startup(() -> partitionCount);

        DescribeGroupsResponseData.DescribedGroup describedGroup = new DescribeGroupsResponseData.DescribedGroup()
            .setGroupId("");
        List<DescribeGroupsResponseData.DescribedGroup> expectedDescribedGroups = Arrays.asList(
            new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId(null)
                .setErrorCode(Errors.INVALID_GROUP_ID.code()),
            describedGroup
        );

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("describe-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(describedGroup)));

        CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>> future =
            service.describeGroups(requestContext(ApiKeys.DESCRIBE_GROUPS), Arrays.asList("", null));

        assertEquals(expectedDescribedGroups, future.get());
    }

    @Test
    public void testDescribeGroupCoordinatorLoadInProgress() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build();
        int partitionCount = 1;
        service.startup(() -> partitionCount);

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("describe-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(
            new CoordinatorLoadInProgressException(null)
        ));

        CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>> future =
            service.describeGroups(requestContext(ApiKeys.DESCRIBE_GROUPS), Collections.singletonList("group-id"));

        assertEquals(
            Collections.singletonList(new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId("group-id")
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
            ),
            future.get()
        );
    }

    @Test
    public void testDescribeGroupsWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>> future = service.describeGroups(
            requestContext(ApiKeys.DESCRIBE_GROUPS),
            Collections.singletonList("group-id")
        );

        assertEquals(
            Collections.singletonList(new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId("group-id")
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            ),
            future.get()
        );
    }

    @ParameterizedTest
    @CsvSource({
        "false, false",
        "false, true",
        "true, false",
        "true, true",
    })
    public void testFetchOffsets(
        boolean fetchAllOffsets,
        boolean requireStable
    ) throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        OffsetFetchRequestData.OffsetFetchRequestGroup request =
            new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group");
        if (!fetchAllOffsets) {
            request
                .setTopics(Collections.singletonList(new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName("foo")
                    .setPartitionIndexes(Collections.singletonList(0))));
        }

        OffsetFetchResponseData.OffsetFetchResponseGroup response =
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId("group")
                .setTopics(Collections.singletonList(new OffsetFetchResponseData.OffsetFetchResponseTopics()
                    .setName("foo")
                    .setPartitions(Collections.singletonList(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                        .setPartitionIndex(0)
                        .setCommittedOffset(100L)))));

        if (requireStable) {
            when(runtime.scheduleWriteOperation(
                ArgumentMatchers.eq(fetchAllOffsets ? "fetch-all-offsets" : "fetch-offsets"),
                ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
                ArgumentMatchers.eq(Duration.ofMillis(5000)),
                ArgumentMatchers.any()
            )).thenReturn(CompletableFuture.completedFuture(response));
        } else {
            when(runtime.scheduleReadOperation(
                ArgumentMatchers.eq(fetchAllOffsets ? "fetch-all-offsets" : "fetch-offsets"),
                ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
                ArgumentMatchers.any()
            )).thenReturn(CompletableFuture.completedFuture(response));
        }

        TriFunction<RequestContext, OffsetFetchRequestData.OffsetFetchRequestGroup, Boolean, CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup>> fetchOffsets =
            fetchAllOffsets ? service::fetchAllOffsets : service::fetchOffsets;
        CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> future = fetchOffsets.apply(
            requestContext(ApiKeys.OFFSET_FETCH),
            request,
            requireStable
        );

        assertEquals(response, future.get(5, TimeUnit.SECONDS));
    }

    @ParameterizedTest
    @CsvSource({
        "false, false",
        "false, true",
        "true, false",
        "true, true",
    })
    public void testFetchOffsetsWhenNotStarted(
        boolean fetchAllOffsets,
        boolean requireStable
    ) throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        OffsetFetchRequestData.OffsetFetchRequestGroup request =
            new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group");
        if (!fetchAllOffsets) {
            request
                .setTopics(Collections.singletonList(new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName("foo")
                    .setPartitionIndexes(Collections.singletonList(0))));
        }

        TriFunction<RequestContext, OffsetFetchRequestData.OffsetFetchRequestGroup, Boolean, CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup>> fetchOffsets =
            fetchAllOffsets ? service::fetchAllOffsets : service::fetchOffsets;
        CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> future = fetchOffsets.apply(
            requestContext(ApiKeys.OFFSET_FETCH),
            request,
            requireStable
        );

        assertEquals(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId("group")
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
            future.get()
        );
    }

    @ParameterizedTest
    @CsvSource({
        "false, UNKNOWN_TOPIC_OR_PARTITION, NOT_COORDINATOR",
        "false, NOT_ENOUGH_REPLICAS, NOT_COORDINATOR",
        "false, REQUEST_TIMED_OUT, NOT_COORDINATOR",
        "false, NOT_LEADER_OR_FOLLOWER, NOT_COORDINATOR",
        "false, KAFKA_STORAGE_ERROR, NOT_COORDINATOR",
        "true, UNKNOWN_TOPIC_OR_PARTITION, NOT_COORDINATOR",
        "true, NOT_ENOUGH_REPLICAS, NOT_COORDINATOR",
        "true, REQUEST_TIMED_OUT, NOT_COORDINATOR",
        "true, NOT_LEADER_OR_FOLLOWER, NOT_COORDINATOR",
        "true, KAFKA_STORAGE_ERROR, NOT_COORDINATOR",
    })
    public void testFetchOffsetsWithWrappedError(
        boolean fetchAllOffsets,
        Errors error,
        Errors expectedError
    ) throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        OffsetFetchRequestData.OffsetFetchRequestGroup request =
            new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group");
        if (!fetchAllOffsets) {
            request
                .setTopics(Collections.singletonList(new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName("foo")
                    .setPartitionIndexes(Collections.singletonList(0))));
        }

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq(fetchAllOffsets ? "fetch-all-offsets" : "fetch-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(new CompletionException(error.exception())));

        TriFunction<RequestContext, OffsetFetchRequestData.OffsetFetchRequestGroup, Boolean, CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup>> fetchOffsets =
            fetchAllOffsets ? service::fetchAllOffsets : service::fetchOffsets;
        CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> future = fetchOffsets.apply(
            requestContext(ApiKeys.OFFSET_FETCH),
            request,
            true
        );

        assertEquals(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId("group")
                .setErrorCode(expectedError.code()),
            future.get()
        );
    }

    @Test
    public void testLeaveGroup() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        LeaveGroupRequestData request = new LeaveGroupRequestData()
            .setGroupId("foo");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-leave"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            new LeaveGroupResponseData()
        ));

        CompletableFuture<LeaveGroupResponseData> future = service.leaveGroup(
            requestContext(ApiKeys.LEAVE_GROUP),
            request
        );

        assertTrue(future.isDone());
        assertEquals(new LeaveGroupResponseData(), future.get());
    }

    @Test
    public void testLeaveGroupThrowsUnknownMemberIdException() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        LeaveGroupRequestData request = new LeaveGroupRequestData()
            .setGroupId("foo")
            .setMembers(Arrays.asList(
                new LeaveGroupRequestData.MemberIdentity()
                    .setMemberId("member-1")
                    .setGroupInstanceId("instance-1"),
                new LeaveGroupRequestData.MemberIdentity()
                    .setMemberId("member-2")
                    .setGroupInstanceId("instance-2")
            ));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-leave"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(
            new UnknownMemberIdException()
        ));

        CompletableFuture<LeaveGroupResponseData> future = service.leaveGroup(
            requestContext(ApiKeys.LEAVE_GROUP),
            request
        );

        assertTrue(future.isDone());
        LeaveGroupResponseData expectedResponse = new LeaveGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setMembers(Arrays.asList(
                new LeaveGroupResponseData.MemberResponse()
                    .setMemberId("member-1")
                    .setGroupInstanceId("instance-1")
                    .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()),
                new LeaveGroupResponseData.MemberResponse()
                    .setMemberId("member-2")
                    .setGroupInstanceId("instance-2")
                    .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
            ));

        assertEquals(expectedResponse, future.get());
    }

    @Test
    public void testLeaveGroupWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        LeaveGroupRequestData request = new LeaveGroupRequestData()
            .setGroupId("foo");

        CompletableFuture<LeaveGroupResponseData> future = service.leaveGroup(
            requestContext(ApiKeys.LEAVE_GROUP),
            request
        );

        assertEquals(
            new LeaveGroupResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
            future.get()
        );
    }

    @Test
    public void testConsumerGroupDescribe() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 2;
        service.startup(() -> partitionCount);

        ConsumerGroupDescribeResponseData.DescribedGroup describedGroup1 = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId("group-id-1");
        ConsumerGroupDescribeResponseData.DescribedGroup describedGroup2 = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId("group-id-2");
        List<ConsumerGroupDescribeResponseData.DescribedGroup> expectedDescribedGroups = Arrays.asList(
            describedGroup1,
            describedGroup2
        );

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("consumer-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(describedGroup1)));

        CompletableFuture<Object> describedGroupFuture = new CompletableFuture<>();
        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("consumer-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)),
            ArgumentMatchers.any()
        )).thenReturn(describedGroupFuture);

        CompletableFuture<List<ConsumerGroupDescribeResponseData.DescribedGroup>> future =
            service.consumerGroupDescribe(requestContext(ApiKeys.CONSUMER_GROUP_DESCRIBE), Arrays.asList("group-id-1", "group-id-2"));

        assertFalse(future.isDone());
        describedGroupFuture.complete(Collections.singletonList(describedGroup2));
        assertEquals(expectedDescribedGroups, future.get());
    }

    @Test
    public void testConsumerGroupDescribeInvalidGroupId() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 1;
        service.startup(() -> partitionCount);

        ConsumerGroupDescribeResponseData.DescribedGroup describedGroup = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId(null)
            .setErrorCode(Errors.INVALID_GROUP_ID.code());
        List<ConsumerGroupDescribeResponseData.DescribedGroup> expectedDescribedGroups = Arrays.asList(
            new ConsumerGroupDescribeResponseData.DescribedGroup()
                .setGroupId(null)
                .setErrorCode(Errors.INVALID_GROUP_ID.code()),
            describedGroup
        );

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("consumer-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(describedGroup)));

        CompletableFuture<List<ConsumerGroupDescribeResponseData.DescribedGroup>> future =
            service.consumerGroupDescribe(requestContext(ApiKeys.CONSUMER_GROUP_DESCRIBE), Arrays.asList("", null));

        assertEquals(expectedDescribedGroups, future.get());
    }

    @Test
    public void testConsumerGroupDescribeCoordinatorLoadInProgress() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 1;
        service.startup(() -> partitionCount);

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("consumer-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(
            new CoordinatorLoadInProgressException(null)
        ));

        CompletableFuture<List<ConsumerGroupDescribeResponseData.DescribedGroup>> future =
            service.consumerGroupDescribe(requestContext(ApiKeys.CONSUMER_GROUP_DESCRIBE), Collections.singletonList("group-id"));

        assertEquals(
            Collections.singletonList(new ConsumerGroupDescribeResponseData.DescribedGroup()
                .setGroupId("group-id")
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
            ),
            future.get()
        );
    }

    @Test
    public void testConsumerGroupDescribeCoordinatorNotActive() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("consumer-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(
            Errors.COORDINATOR_NOT_AVAILABLE.exception()
        ));

        CompletableFuture<List<ConsumerGroupDescribeResponseData.DescribedGroup>> future =
            service.consumerGroupDescribe(requestContext(ApiKeys.CONSUMER_GROUP_DESCRIBE), Collections.singletonList("group-id"));

        assertEquals(
            Collections.singletonList(new ConsumerGroupDescribeResponseData.DescribedGroup()
                .setGroupId("group-id")
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            ),
            future.get()
        );
    }

    @Test
    public void testDeleteOffsets() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build(true);

        OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection requestTopicCollection =
            new OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection(Collections.singletonList(
                new OffsetDeleteRequestData.OffsetDeleteRequestTopic()
                    .setName(TOPIC_NAME)
                    .setPartitions(Collections.singletonList(
                        new OffsetDeleteRequestData.OffsetDeleteRequestPartition().setPartitionIndex(0)
                    ))
            ).iterator());
        OffsetDeleteRequestData request = new OffsetDeleteRequestData()
            .setGroupId("group")
            .setTopics(requestTopicCollection);

        OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection responsePartitionCollection =
            new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection(Collections.singletonList(
                new OffsetDeleteResponseData.OffsetDeleteResponsePartition().setPartitionIndex(0)
            ).iterator());
        OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection responseTopicCollection =
            new OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection(Collections.singletonList(
                new OffsetDeleteResponseData.OffsetDeleteResponseTopic().setPartitions(responsePartitionCollection)
            ).iterator());
        OffsetDeleteResponseData response = new OffsetDeleteResponseData()
            .setTopics(responseTopicCollection);

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<OffsetDeleteResponseData> future = service.deleteOffsets(
            requestContext(ApiKeys.OFFSET_DELETE),
            request,
            BufferSupplier.NO_CACHING
        );

        assertTrue(future.isDone());
        assertEquals(response, future.get());
    }

    @Test
    public void testDeleteOffsetsInvalidGroupId() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build(true);

        OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection requestTopicCollection =
            new OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection(Collections.singletonList(
                new OffsetDeleteRequestData.OffsetDeleteRequestTopic()
                    .setName(TOPIC_NAME)
                    .setPartitions(Collections.singletonList(
                        new OffsetDeleteRequestData.OffsetDeleteRequestPartition().setPartitionIndex(0)
                    ))
            ).iterator());
        OffsetDeleteRequestData request = new OffsetDeleteRequestData().setGroupId("")
            .setTopics(requestTopicCollection);

        OffsetDeleteResponseData response = new OffsetDeleteResponseData()
            .setErrorCode(Errors.INVALID_GROUP_ID.code());

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<OffsetDeleteResponseData> future = service.deleteOffsets(
            requestContext(ApiKeys.OFFSET_DELETE),
            request,
            BufferSupplier.NO_CACHING
        );

        assertTrue(future.isDone());
        assertEquals(response, future.get());
    }

    @ParameterizedTest
    @MethodSource("testGroupHeartbeatWithExceptionSource")
    public void testDeleteOffsetsWithException(
        Throwable exception,
        short expectedErrorCode
    ) throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build(true);

        OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection requestTopicCollection =
            new OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection(Collections.singletonList(
                new OffsetDeleteRequestData.OffsetDeleteRequestTopic()
                    .setName(TOPIC_NAME)
                    .setPartitions(Collections.singletonList(
                        new OffsetDeleteRequestData.OffsetDeleteRequestPartition().setPartitionIndex(0)
                    ))
            ).iterator());
        OffsetDeleteRequestData request = new OffsetDeleteRequestData()
            .setGroupId("group")
            .setTopics(requestTopicCollection);

        OffsetDeleteResponseData response = new OffsetDeleteResponseData()
            .setErrorCode(expectedErrorCode);

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(exception));

        CompletableFuture<OffsetDeleteResponseData> future = service.deleteOffsets(
            requestContext(ApiKeys.OFFSET_DELETE),
            request,
            BufferSupplier.NO_CACHING
        );

        assertTrue(future.isDone());
        assertEquals(response, future.get());
    }

    @Test
    public void testDeleteOffsetsWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        OffsetDeleteRequestData request = new OffsetDeleteRequestData()
            .setGroupId("foo");

        CompletableFuture<OffsetDeleteResponseData> future = service.deleteOffsets(
            requestContext(ApiKeys.OFFSET_DELETE),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(
            new OffsetDeleteResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
            future.get()
        );
    }

    @Test
    public void testDeleteGroups() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build();
        service.startup(() -> 3);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection1 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        DeleteGroupsResponseData.DeletableGroupResult result1 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("group-id-1");
        resultCollection1.add(result1);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection2 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        DeleteGroupsResponseData.DeletableGroupResult result2 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("group-id-2");
        resultCollection2.add(result2);

        DeleteGroupsResponseData.DeletableGroupResult result3 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("group-id-3")
            .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code());

        DeleteGroupsResponseData.DeletableGroupResultCollection expectedResultCollection =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        expectedResultCollection.addAll(Arrays.asList(
            new DeleteGroupsResponseData.DeletableGroupResult().setGroupId(null).setErrorCode(Errors.INVALID_GROUP_ID.code()),
            result2.duplicate(),
            result3.duplicate(),
            result1.duplicate()
        ));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-share-groups"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(Map.of()));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 2)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(resultCollection1));

        CompletableFuture<Object> resultCollectionFuture = new CompletableFuture<>();
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(resultCollectionFuture);

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(Errors.COORDINATOR_LOAD_IN_PROGRESS.exception()));

        List<String> groupIds = Arrays.asList("group-id-1", "group-id-2", "group-id-3", null);
        CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future =
            service.deleteGroups(requestContext(ApiKeys.DELETE_GROUPS), groupIds, BufferSupplier.NO_CACHING);

        assertFalse(future.isDone());
        resultCollectionFuture.complete(resultCollection2);

        assertTrue(future.isDone());
        assertEquals(expectedResultCollection, future.get());
    }

    @Test
    public void testDeleteWithShareGroups() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(Persister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .setPersister(persister)
            .build();
        service.startup(() -> 3);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection1 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        // share group
        DeleteGroupsResponseData.DeletableGroupResult result1 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("share-group-id-1");
        resultCollection1.add(result1);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection2 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        // non-share group
        DeleteGroupsResponseData.DeletableGroupResult result2 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("group-id-2");
        resultCollection2.add(result2);

        // null
        DeleteGroupsResponseData.DeletableGroupResult result3 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId(null)
            .setErrorCode(Errors.INVALID_GROUP_ID.code());

        DeleteGroupsResponseData.DeletableGroupResultCollection expectedResultCollection =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        expectedResultCollection.addAll(List.of(
            result3.duplicate(),
            result2.duplicate(),
            result1.duplicate()
        ));

        Uuid shareGroupTopicId = Uuid.randomUuid();

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-share-groups"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            Map.of("share-group-id-1", Map.entry(createDeleteShareRequest("share-group-id-1", shareGroupTopicId, List.of(0, 1)), Errors.NONE))
        )).thenReturn(CompletableFuture.completedFuture(Map.of()));   // non-share group

        when(persister.deleteState(any())).thenReturn(CompletableFuture.completedFuture(
            new DeleteShareGroupStateResult.Builder()
                .setTopicsData(List.of(
                    new TopicData<>(
                        shareGroupTopicId,
                        List.of(
                            PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message()),
                            PartitionFactory.newPartitionErrorData(1, Errors.NONE.code(), Errors.NONE.message())
                        ))
                ))
                .build()
        ));

        // share-group-id-1
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(resultCollection1));

        // group-id-2
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(resultCollection2));

        List<String> groupIds = Arrays.asList("share-group-id-1", "group-id-2", null);
        CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future =
            service.deleteGroups(requestContext(ApiKeys.DELETE_GROUPS), groupIds, BufferSupplier.NO_CACHING);

        future.getNow(null);
        assertEquals(expectedResultCollection, future.get());
        verify(persister, times(1)).deleteState(any());
    }

    @Test
    public void testDeleteShareGroupPersisterError() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(Persister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .setPersister(persister)
            .build();
        service.startup(() -> 3);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection1 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        // share group err
        DeleteGroupsResponseData.DeletableGroupResult result1 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("share-group-id-1")
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
        resultCollection1.add(result1);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection2 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        // share group success
        DeleteGroupsResponseData.DeletableGroupResult result2 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("share-group-id-2");
        resultCollection2.add(result2);

        DeleteGroupsResponseData.DeletableGroupResultCollection expectedResultCollection =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        expectedResultCollection.addAll(Arrays.asList(
            result1.duplicate(),
            result2.duplicate()));

        Uuid shareGroupTopicId = Uuid.randomUuid();
        Uuid shareGroupTopicId2 = Uuid.randomUuid();
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-share-groups"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            Map.of("share-group-id-1", Map.entry(createDeleteShareRequest("share-group-id-1", shareGroupTopicId, List.of(0, 1)), Errors.NONE))
        )).thenReturn(CompletableFuture.completedFuture(
            Map.of("share-group-id-2", Map.entry(createDeleteShareRequest("share-group-id-2", shareGroupTopicId2, List.of(0, 1)), Errors.NONE))
        ));

        when(persister.deleteState(any())).thenReturn(CompletableFuture.completedFuture(
            new DeleteShareGroupStateResult.Builder()
                .setTopicsData(List.of(
                    new TopicData<>(
                        shareGroupTopicId,
                        List.of(
                            PartitionFactory.newPartitionErrorData(0, Errors.UNKNOWN_SERVER_ERROR.code(), Errors.UNKNOWN_SERVER_ERROR.message()),
                            PartitionFactory.newPartitionErrorData(1, Errors.UNKNOWN_SERVER_ERROR.code(), Errors.UNKNOWN_SERVER_ERROR.message())
                        ))
                ))
                .build()
        )).thenReturn(CompletableFuture.completedFuture(
            new DeleteShareGroupStateResult.Builder()
                .setTopicsData(List.of(
                    new TopicData<>(
                        shareGroupTopicId2,
                        List.of(
                            PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message()),
                            PartitionFactory.newPartitionErrorData(1, Errors.NONE.code(), Errors.NONE.message())
                        ))
                ))
                .build()
        ));

        // share-group-id-1
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(resultCollection1));

        // share-group-id-2
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 2)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(resultCollection2));

        List<String> groupIds = List.of("share-group-id-1", "share-group-id-2");
        CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future =
            service.deleteGroups(requestContext(ApiKeys.DELETE_GROUPS), groupIds, BufferSupplier.NO_CACHING);

        future.getNow(null);
        assertEquals(expectedResultCollection, future.get());
        verify(persister, times(2)).deleteState(any());
    }

    @Test
    public void testDeleteShareGroupCoordinatorShareSpecificWriteError() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(Persister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .setPersister(persister)
            .build();
        service.startup(() -> 3);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection1 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        // share group err
        DeleteGroupsResponseData.DeletableGroupResult result1 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("share-group-id-1")
            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code());
        resultCollection1.add(result1);

        DeleteGroupsResponseData.DeletableGroupResultCollection expectedResultCollection =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        expectedResultCollection.add(
            result1.duplicate()
        );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-share-groups"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(
            Errors.COORDINATOR_NOT_AVAILABLE.exception()
        ));

        // share-group-id-1
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(resultCollection1));

        List<String> groupIds = List.of("share-group-id-1");
        CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future =
            service.deleteGroups(requestContext(ApiKeys.DELETE_GROUPS), groupIds, BufferSupplier.NO_CACHING);

        future.getNow(null);
        assertEquals(expectedResultCollection, future.get());
        verify(persister, times(0)).deleteState(any());
    }

    @Test
    public void testDeleteShareGroupNotEmptyError() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(Persister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .setPersister(persister)
            .build();
        service.startup(() -> 3);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection1 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        // share group err
        DeleteGroupsResponseData.DeletableGroupResult result1 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("share-group-id-1")
            .setErrorCode(Errors.forException(new GroupNotEmptyException("bad stuff")).code());
        resultCollection1.add(result1);

        DeleteGroupsResponseData.DeletableGroupResultCollection expectedResultCollection =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        expectedResultCollection.add(
            result1.duplicate()
        );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-share-groups"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            Map.of("share-group-id-1", Map.entry(EMPTY_PARAMS, Errors.forException(new GroupNotEmptyException("bad stuff"))))
        ));

        List<String> groupIds = List.of("share-group-id-1");
        CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future =
            service.deleteGroups(requestContext(ApiKeys.DELETE_GROUPS), groupIds, BufferSupplier.NO_CACHING);

        future.getNow(null);
        assertEquals(expectedResultCollection, future.get());
        // If there is error creating share group delete req
        // neither persister call nor general delete groups call is made.
        verify(persister, times(0)).deleteState(any());
        verify(runtime, times(0)).scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        );
    }

    @Test
    public void testDeleteShareGroupCoordinatorGeneralWriteError() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(Persister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .setPersister(persister)
            .build();
        service.startup(() -> 3);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection1 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        // share group err
        DeleteGroupsResponseData.DeletableGroupResult result1 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("share-group-id-1")
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
        resultCollection1.add(result1);

        DeleteGroupsResponseData.DeletableGroupResultCollection expectedResultCollection =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        expectedResultCollection.add(
            result1.duplicate()
        );

        Uuid shareGroupTopicId = Uuid.randomUuid();
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-share-groups"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            Map.of("share-group-id-1", Map.entry(createDeleteShareRequest("share-group-id-1", shareGroupTopicId, List.of(0, 1)), Errors.NONE))
        ));

        // share-group-id-1
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(Errors.CLUSTER_AUTHORIZATION_FAILED.exception()));

        when(persister.deleteState(any())).thenReturn(CompletableFuture.completedFuture(new DeleteShareGroupStateResult.Builder()
            .setTopicsData(List.of(
                new TopicData<>(
                    shareGroupTopicId,
                    List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message()),
                        PartitionFactory.newPartitionErrorData(1, Errors.NONE.code(), Errors.NONE.message())
                    ))
            ))
            .build()
        ));

        List<String> groupIds = List.of("share-group-id-1");
        CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future =
            service.deleteGroups(requestContext(ApiKeys.DELETE_GROUPS), groupIds, BufferSupplier.NO_CACHING);

        future.getNow(null);
        assertEquals(expectedResultCollection, future.get());
        verify(persister, times(1)).deleteState(any());
    }

    @ParameterizedTest
    @MethodSource("testGroupHeartbeatWithExceptionSource")
    public void testDeleteGroupsWithException(
        Throwable exception,
        short expectedErrorCode
    ) throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build(true);

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-share-groups"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(Map.of()));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(exception));

        CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future =
            service.deleteGroups(
                requestContext(ApiKeys.DELETE_GROUPS),
                Collections.singletonList("group-id"),
                BufferSupplier.NO_CACHING
            );

        assertEquals(
            new DeleteGroupsResponseData.DeletableGroupResultCollection(Collections.singletonList(
                new DeleteGroupsResponseData.DeletableGroupResult()
                    .setGroupId("group-id")
                    .setErrorCode(expectedErrorCode)
            ).iterator()),
            future.get()
        );
    }

    @Test
    public void testDeleteGroupsWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build();

        CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future = service.deleteGroups(
            requestContext(ApiKeys.DELETE_GROUPS),
            Collections.singletonList("foo"),
            BufferSupplier.NO_CACHING
        );

        assertEquals(
            new DeleteGroupsResponseData.DeletableGroupResultCollection(
                Collections.singletonList(new DeleteGroupsResponseData.DeletableGroupResult()
                    .setGroupId("foo")
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                ).iterator()
            ),
            future.get()
        );
    }

    @Test
    public void testCommitTransactionalOffsetsWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        TxnOffsetCommitRequestData request = new TxnOffsetCommitRequestData()
            .setGroupId("foo")
            .setTransactionalId("transactional-id")
            .setMemberId("member-id")
            .setGenerationId(10)
            .setTopics(Collections.singletonList(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                .setName(TOPIC_NAME)
                .setPartitions(Collections.singletonList(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                    .setPartitionIndex(0)
                    .setCommittedOffset(100)))));

        CompletableFuture<TxnOffsetCommitResponseData> future = service.commitTransactionalOffsets(
            requestContext(ApiKeys.TXN_OFFSET_COMMIT),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(
            new TxnOffsetCommitResponseData()
                .setTopics(Collections.singletonList(new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                    .setName(TOPIC_NAME)
                    .setPartitions(Collections.singletonList(new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                        .setPartitionIndex(0)
                        .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()))))),
            future.get()
        );
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {""})
    public void testCommitTransactionalOffsetsWithInvalidGroupId(String groupId) throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        TxnOffsetCommitRequestData request = new TxnOffsetCommitRequestData()
            .setGroupId(groupId)
            .setTransactionalId("transactional-id")
            .setMemberId("member-id")
            .setGenerationId(10)
            .setTopics(Collections.singletonList(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                .setName(TOPIC_NAME)
                .setPartitions(Collections.singletonList(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                    .setPartitionIndex(0)
                    .setCommittedOffset(100)))));

        CompletableFuture<TxnOffsetCommitResponseData> future = service.commitTransactionalOffsets(
            requestContext(ApiKeys.TXN_OFFSET_COMMIT),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(
            new TxnOffsetCommitResponseData()
                .setTopics(Collections.singletonList(new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                    .setName(TOPIC_NAME)
                    .setPartitions(Collections.singletonList(new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                        .setPartitionIndex(0)
                        .setErrorCode(Errors.INVALID_GROUP_ID.code()))))),
            future.get()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {4, 5})
    public void testCommitTransactionalOffsets(Short txnOffsetCommitVersion) throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        TxnOffsetCommitRequestData request = new TxnOffsetCommitRequestData()
            .setGroupId("foo")
            .setTransactionalId("transactional-id")
            .setProducerId(10L)
            .setProducerEpoch((short) 5)
            .setMemberId("member-id")
            .setGenerationId(10)
            .setTopics(Collections.singletonList(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                .setName(TOPIC_NAME)
                .setPartitions(Collections.singletonList(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                    .setPartitionIndex(0)
                    .setCommittedOffset(100)))));

        TxnOffsetCommitResponseData response = new TxnOffsetCommitResponseData()
            .setTopics(Collections.singletonList(new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                .setName(TOPIC_NAME)
                .setPartitions(Collections.singletonList(new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                    .setPartitionIndex(0)
                    .setErrorCode(Errors.NONE.code())))));

        when(runtime.scheduleTransactionalWriteOperation(
            ArgumentMatchers.eq("txn-commit-offset"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq("transactional-id"),
            ArgumentMatchers.eq(10L),
            ArgumentMatchers.eq((short) 5),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<TxnOffsetCommitResponseData> future = service.commitTransactionalOffsets(
            requestContext(ApiKeys.TXN_OFFSET_COMMIT, txnOffsetCommitVersion),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(response, future.get());
    }

    @ParameterizedTest
    @CsvSource({
        "NOT_ENOUGH_REPLICAS, COORDINATOR_NOT_AVAILABLE",
        "NETWORK_EXCEPTION, COORDINATOR_LOAD_IN_PROGRESS"
    })
    public void testCommitTransactionalOffsetsWithWrappedError(
        Errors error,
        Errors expectedError
    ) throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        TxnOffsetCommitRequestData request = new TxnOffsetCommitRequestData()
            .setGroupId("foo")
            .setTransactionalId("transactional-id")
            .setProducerId(10L)
            .setProducerEpoch((short) 5)
            .setMemberId("member-id")
            .setGenerationId(10)
            .setTopics(Collections.singletonList(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                .setName(TOPIC_NAME)
                .setPartitions(Collections.singletonList(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                    .setPartitionIndex(0)
                    .setCommittedOffset(100)))));

        TxnOffsetCommitResponseData response = new TxnOffsetCommitResponseData()
            .setTopics(Collections.singletonList(new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                .setName(TOPIC_NAME)
                .setPartitions(Collections.singletonList(new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                    .setPartitionIndex(0)
                    .setErrorCode(expectedError.code())))));

        when(runtime.scheduleTransactionalWriteOperation(
            ArgumentMatchers.eq("txn-commit-offset"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq("transactional-id"),
            ArgumentMatchers.eq(10L),
            ArgumentMatchers.eq((short) 5),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(new CompletionException(error.exception())));

        CompletableFuture<TxnOffsetCommitResponseData> future = service.commitTransactionalOffsets(
            requestContext(ApiKeys.TXN_OFFSET_COMMIT),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(response, future.get());
    }

    @Test
    public void testCompleteTransaction() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        when(runtime.scheduleTransactionCompletion(
            ArgumentMatchers.eq("write-txn-marker"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(100L),
            ArgumentMatchers.eq((short) 5),
            ArgumentMatchers.eq(10),
            ArgumentMatchers.eq(TransactionResult.COMMIT),
            ArgumentMatchers.eq(Duration.ofMillis(100))
        )).thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> future = service.completeTransaction(
            new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0),
            100L,
            (short) 5,
            10,
            TransactionResult.COMMIT,
            Duration.ofMillis(100)
        );

        assertNull(future.get());
    }

    @Test
    public void testCompleteTransactionWhenNotCoordinatorServiceStarted() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        CompletableFuture<Void> future = service.completeTransaction(
            new TopicPartition("foo", 0),
            100L,
            (short) 5,
            10,
            TransactionResult.COMMIT,
            Duration.ofMillis(100)
        );

        assertFutureThrows(CoordinatorNotAvailableException.class, future);
    }

    @Test
    public void testCompleteTransactionWithUnexpectedPartition() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        CompletableFuture<Void> future = service.completeTransaction(
            new TopicPartition("foo", 0),
            100L,
            (short) 5,
            10,
            TransactionResult.COMMIT,
            Duration.ofMillis(100)
        );

        assertFutureThrows(IllegalStateException.class, future);
    }

    @Test
    public void testOnPartitionsDeleted() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        service.startup(() -> 3);

        when(runtime.scheduleWriteAllOperation(
            ArgumentMatchers.eq("on-partition-deleted"),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(Arrays.asList(
            CompletableFuture.completedFuture(null),
            CompletableFuture.completedFuture(null),
            FutureUtils.failedFuture(Errors.COORDINATOR_LOAD_IN_PROGRESS.exception())
        ));

        // The exception is logged and swallowed.
        assertDoesNotThrow(() ->
            service.onPartitionsDeleted(
                Collections.singletonList(new TopicPartition("foo", 0)),
                BufferSupplier.NO_CACHING
            )
        );
    }

    @Test
    public void testOnPartitionsDeletedWhenServiceIsNotStarted() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        assertThrows(CoordinatorNotAvailableException.class, () -> service.onPartitionsDeleted(
            Collections.singletonList(new TopicPartition("foo", 0)),
            BufferSupplier.NO_CACHING
        ));
    }

    @Test
    public void testShareGroupHeartbeatWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        ShareGroupHeartbeatRequestData request = new ShareGroupHeartbeatRequestData()
            .setGroupId("foo");

        CompletableFuture<ShareGroupHeartbeatResponseData> future = service.shareGroupHeartbeat(
            requestContext(ApiKeys.SHARE_GROUP_HEARTBEAT),
            request
        );

        assertEquals(new ShareGroupHeartbeatResponseData().setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()), future.get());
    }

    @Test
    public void testShareGroupHeartbeat() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        ShareGroupHeartbeatRequestData request = new ShareGroupHeartbeatRequestData()
            .setGroupId("foo");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("share-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            new ShareGroupHeartbeatResponseData()
        ));

        CompletableFuture<ShareGroupHeartbeatResponseData> future = service.shareGroupHeartbeat(
            requestContext(ApiKeys.SHARE_GROUP_HEARTBEAT),
            request
        );

        assertEquals(new ShareGroupHeartbeatResponseData(), future.get(5, TimeUnit.SECONDS));
    }

    @ParameterizedTest
    @MethodSource("testGroupHeartbeatWithExceptionSource")
    public void testShareGroupHeartbeatWithException(
        Throwable exception,
        short expectedErrorCode,
        String expectedErrorMessage
    ) throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        ShareGroupHeartbeatRequestData request = new ShareGroupHeartbeatRequestData()
            .setGroupId("foo");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("share-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(exception));

        CompletableFuture<ShareGroupHeartbeatResponseData> future = service.shareGroupHeartbeat(
            requestContext(ApiKeys.SHARE_GROUP_HEARTBEAT),
            request
        );

        assertEquals(
            new ShareGroupHeartbeatResponseData()
                .setErrorCode(expectedErrorCode)
                .setErrorMessage(expectedErrorMessage),
            future.get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testShareGroupDescribe() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 2;
        service.startup(() -> partitionCount);

        ShareGroupDescribeResponseData.DescribedGroup describedGroup1 = new ShareGroupDescribeResponseData.DescribedGroup()
            .setGroupId("share-group-id-1");
        ShareGroupDescribeResponseData.DescribedGroup describedGroup2 = new ShareGroupDescribeResponseData.DescribedGroup()
            .setGroupId("share-group-id-2");
        List<ShareGroupDescribeResponseData.DescribedGroup> expectedDescribedGroups = Arrays.asList(
            describedGroup1,
            describedGroup2
        );

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("share-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(describedGroup1)));

        CompletableFuture<Object> describedGroupFuture = new CompletableFuture<>();
        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("share-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)),
            ArgumentMatchers.any()
        )).thenReturn(describedGroupFuture);

        CompletableFuture<List<ShareGroupDescribeResponseData.DescribedGroup>> future =
            service.shareGroupDescribe(requestContext(ApiKeys.SHARE_GROUP_DESCRIBE), Arrays.asList("share-group-id-1", "share-group-id-2"));

        assertFalse(future.isDone());
        describedGroupFuture.complete(Collections.singletonList(describedGroup2));
        assertEquals(expectedDescribedGroups, future.get());
    }

    @Test
    public void testShareGroupDescribeInvalidGroupId() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 1;
        service.startup(() -> partitionCount);

        ShareGroupDescribeResponseData.DescribedGroup describedGroup = new ShareGroupDescribeResponseData.DescribedGroup()
            .setGroupId(null)
            .setErrorCode(Errors.INVALID_GROUP_ID.code());
        List<ShareGroupDescribeResponseData.DescribedGroup> expectedDescribedGroups = Arrays.asList(
            new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId(null)
                .setErrorCode(Errors.INVALID_GROUP_ID.code()),
            describedGroup
        );

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("share-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(describedGroup)));

        CompletableFuture<List<ShareGroupDescribeResponseData.DescribedGroup>> future =
            service.shareGroupDescribe(requestContext(ApiKeys.SHARE_GROUP_DESCRIBE), Arrays.asList("", null));

        assertEquals(expectedDescribedGroups, future.get());
    }

    @Test
    public void testShareGroupDescribeCoordinatorLoadInProgress() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 1;
        service.startup(() -> partitionCount);

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("share-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(
            new CoordinatorLoadInProgressException(null)
        ));

        CompletableFuture<List<ShareGroupDescribeResponseData.DescribedGroup>> future =
            service.shareGroupDescribe(requestContext(ApiKeys.SHARE_GROUP_DESCRIBE), Collections.singletonList("share-group-id"));

        assertEquals(
            Collections.singletonList(new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId("share-group-id")
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
            ),
            future.get()
        );
    }

    @Test
    public void testShareGroupDescribeCoordinatorNotActive() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("share-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(
            Errors.COORDINATOR_NOT_AVAILABLE.exception()
        ));

        CompletableFuture<List<ShareGroupDescribeResponseData.DescribedGroup>> future =
            service.shareGroupDescribe(requestContext(ApiKeys.SHARE_GROUP_DESCRIBE), Collections.singletonList("share-group-id"));

        assertEquals(
            Collections.singletonList(new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId("share-group-id")
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            ),
            future.get()
        );
    }

    @Test
    public void testDescribeShareGroupOffsetsWithNoOpPersister() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);
        service.startup(() -> 1);

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseData = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
            .setGroupId("share-group-id")
            .setTopics(
                List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic()
                    .setTopicName(TOPIC_NAME)
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                        .setPartitionIndex(partition)
                        .setStartOffset(PartitionFactory.UNINITIALIZED_START_OFFSET)
                        .setErrorCode(PartitionFactory.DEFAULT_ERROR_CODE)
                        .setErrorMessage(PartitionFactory.DEFAULT_ERR_MESSAGE))))
            );

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDescribeShareGroupOffsetsWithDefaultPersister() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        ReadShareGroupStateSummaryRequestData readShareGroupStateSummaryRequestData = new ReadShareGroupStateSummaryRequestData()
            .setGroupId("share-group-id")
            .setTopics(List.of(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new ReadShareGroupStateSummaryRequestData.PartitionData()
                        .setPartition(partition)))));

        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseData = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
            .setGroupId("share-group-id")
            .setTopics(
                List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic()
                    .setTopicName(TOPIC_NAME)
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                        .setPartitionIndex(partition)
                        .setStartOffset(21)
                        .setErrorCode(Errors.NONE.code())
                        .setErrorMessage(Errors.NONE.message()))))
            );

        ReadShareGroupStateSummaryResponseData readShareGroupStateSummaryResponseData = new ReadShareGroupStateSummaryResponseData()
            .setResults(
                List.of(new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new ReadShareGroupStateSummaryResponseData.PartitionResult()
                        .setPartition(partition)
                        .setStartOffset(21)
                        .setStateEpoch(1)
                        .setErrorCode(Errors.NONE.code())
                        .setErrorMessage(Errors.NONE.message())))
                )
            );

        ReadShareGroupStateSummaryParameters readShareGroupStateSummaryParameters = ReadShareGroupStateSummaryParameters.from(readShareGroupStateSummaryRequestData);
        ReadShareGroupStateSummaryResult readShareGroupStateSummaryResult = ReadShareGroupStateSummaryResult.from(readShareGroupStateSummaryResponseData);
        when(persister.readSummary(
            ArgumentMatchers.eq(readShareGroupStateSummaryParameters)
            )).thenReturn(CompletableFuture.completedFuture(readShareGroupStateSummaryResult));

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDescribeShareGroupOffsetsNonexistentTopicWithDefaultPersister() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName("badtopic")
                .setPartitions(List.of(partition))
            ));

        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseData = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
            .setGroupId("share-group-id")
            .setTopics(
                List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic()
                    .setTopicName("badtopic")
                    .setPartitions(List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                        .setPartitionIndex(partition)
                        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                        .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message()))))
            );

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDescribeShareGroupOffsetsWithDefaultPersisterThrowsError() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        when(persister.readSummary(ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.failedFuture(new Exception("Unable to validate read state summary request")));

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);
        assertFutureThrows(Exception.class, future, "Unable to validate read state summary request");
    }

    @Test
    public void testDescribeShareGroupOffsetsWithDefaultPersisterNullResult() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        when(persister.readSummary(ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);
        assertFutureThrows(IllegalStateException.class, future, "Result is null for the read state summary");
    }

    @Test
    public void testDescribeShareGroupOffsetsWithDefaultPersisterNullTopicData() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        ReadShareGroupStateSummaryResult readShareGroupStateSummaryResult =
            new ReadShareGroupStateSummaryResult.Builder().setTopicsData(null).build();

        when(persister.readSummary(ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.completedFuture(readShareGroupStateSummaryResult));

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);
        assertFutureThrows(IllegalStateException.class, future, "Result is null for the read state summary");
    }

    @Test
    public void testDescribeShareGroupOffsetsCoordinatorNotActive() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseData = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
            .setGroupId("share-group-id")
            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            .setErrorMessage(Errors.COORDINATOR_NOT_AVAILABLE.message());

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDescribeShareGroupOffsetsMetadataImageNull() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        // Forcing a null Metadata Image
        service.onNewMetadataImage(null, null);

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseData = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
            .setGroupId("share-group-id")
            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            .setErrorMessage(Errors.COORDINATOR_NOT_AVAILABLE.message());

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @FunctionalInterface
    private interface TriFunction<A, B, C, R> {
        R apply(A a, B b, C c);
    }

    private static class GroupCoordinatorServiceBuilder {
        private final LogContext logContext = new LogContext();
        private final GroupConfigManager configManager = createConfigManager();
        private GroupCoordinatorConfig config;
        private CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime;
        private GroupCoordinatorMetrics metrics = new GroupCoordinatorMetrics();
        private Persister persister = new NoOpStatePersister();
        private MetadataImage metadataImage = null;

        GroupCoordinatorService build() {
            return build(false);
        }

        GroupCoordinatorService build(boolean serviceStartup) {
            if (metadataImage == null) {
                metadataImage = mock(MetadataImage.class);
            }

            GroupCoordinatorService service = new GroupCoordinatorService(
                logContext,
                config,
                runtime,
                metrics,
                configManager,
                persister
            );

            if (serviceStartup) {
                service.startup(() -> 1);
                service.onNewMetadataImage(metadataImage, null);
            }
            when(metadataImage.topics()).thenReturn(mock(TopicsImage.class));
            when(metadataImage.topics().topicIdToNameView()).thenReturn(Map.of(TOPIC_ID, TOPIC_NAME));
            when(metadataImage.topics().topicNameToIdView()).thenReturn(Map.of(TOPIC_NAME, TOPIC_ID));

            return service;
        }

        public GroupCoordinatorServiceBuilder setConfig(GroupCoordinatorConfig config) {
            this.config = config;
            return this;
        }

        public GroupCoordinatorServiceBuilder setRuntime(CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime) {
            this.runtime = runtime;
            return this;
        }

        public GroupCoordinatorServiceBuilder setPersister(Persister persister) {
            this.persister = persister;
            return this;
        }

        public GroupCoordinatorServiceBuilder setMetrics(GroupCoordinatorMetrics metrics) {
            this.metrics = metrics;
            return this;
        }
    }

    private static DeleteShareGroupStateParameters createDeleteShareRequest(String groupId, Uuid topic, List<Integer> partitions) {
        TopicData<PartitionIdData> topicData = new TopicData<>(topic,
            partitions.stream().map(PartitionFactory::newPartitionIdData).toList()
        );

        return new DeleteShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionIdData>()
                .setGroupId(groupId)
                .setTopicsData(List.of(topicData))
                .build())
            .build();
    }
}
