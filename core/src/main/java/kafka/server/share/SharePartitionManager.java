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
package kafka.server.share;

import kafka.cluster.PartitionListener;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.FencedStateEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareRequestMetadata;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.server.share.CachedSharePartition;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch;
import org.apache.kafka.server.share.context.FinalContext;
import org.apache.kafka.server.share.context.ShareFetchContext;
import org.apache.kafka.server.share.context.ShareSessionContext;
import org.apache.kafka.server.share.fetch.DelayedShareFetchGroupKey;
import org.apache.kafka.server.share.fetch.DelayedShareFetchKey;
import org.apache.kafka.server.share.fetch.DelayedShareFetchPartitionKey;
import org.apache.kafka.server.share.fetch.PartitionRotateStrategy;
import org.apache.kafka.server.share.fetch.PartitionRotateStrategy.PartitionRotateMetadata;
import org.apache.kafka.server.share.fetch.ShareFetch;
import org.apache.kafka.server.share.metrics.ShareGroupMetrics;
import org.apache.kafka.server.share.persister.Persister;
import org.apache.kafka.server.share.session.ShareSession;
import org.apache.kafka.server.share.session.ShareSessionCache;
import org.apache.kafka.server.share.session.ShareSessionKey;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * The SharePartitionManager is responsible for managing the SharePartitions and ShareSessions.
 * It is responsible for fetching messages from the log and acknowledging the messages.
 */
public class SharePartitionManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SharePartitionManager.class);

    /**
     * The partition cache map is used to store the SharePartition objects for each share group topic-partition.
     */
    private final Map<SharePartitionKey, SharePartition> partitionCacheMap;

    /**
     * The replica manager is used to fetch messages from the log.
     */
    private final ReplicaManager replicaManager;

    /**
     * The time instance is used to get the current time.
     */
    private final Time time;

    /**
     * The share session cache stores the share sessions.
     */
    private final ShareSessionCache cache;

    /**
     * The group config manager is used to retrieve the values for dynamic group configurations
     */
    private final GroupConfigManager groupConfigManager;

    /**
     * The default record lock duration is the time in milliseconds that a record lock is held for.
     * This default value can be overridden by a group-specific configuration.
     */
    private final int defaultRecordLockDurationMs;

    /**
     * The timer is used to schedule the records lock timeout.
     */
    private final Timer timer;

    /**
     * The max in flight messages is the maximum number of messages that can be in flight at any one time per share-partition.
     */
    private final int maxInFlightMessages;

    /**
     * The max delivery count is the maximum number of times a message can be delivered before it is considered to be archived.
     */
    private final int maxDeliveryCount;

    /**
     * The persister is used to persist the share partition state.
     */
    private final Persister persister;

    /**
     * Class with methods to record share group metrics.
     */
    private final ShareGroupMetrics shareGroupMetrics;

    /**
     * The broker topic stats is used to record the broker topic metrics for share group.
     */
    private final BrokerTopicStats brokerTopicStats;

    /**
     * The max fetch records is the maximum number of records that can be fetched by a share fetch request.
     */
    private final int maxFetchRecords;

    public SharePartitionManager(
        ReplicaManager replicaManager,
        Time time,
        ShareSessionCache cache,
        int defaultRecordLockDurationMs,
        int maxDeliveryCount,
        int maxInFlightMessages,
        int maxFetchRecords,
        Persister persister,
        GroupConfigManager groupConfigManager,
        BrokerTopicStats brokerTopicStats
    ) {
        this(replicaManager,
            time,
            cache,
            new ConcurrentHashMap<>(),
            defaultRecordLockDurationMs,
            maxDeliveryCount,
            maxInFlightMessages,
            maxFetchRecords,
            persister,
            groupConfigManager,
            new ShareGroupMetrics(time),
            brokerTopicStats
        );
    }

    private SharePartitionManager(
        ReplicaManager replicaManager,
        Time time,
        ShareSessionCache cache,
        Map<SharePartitionKey, SharePartition> partitionCacheMap,
        int defaultRecordLockDurationMs,
        int maxDeliveryCount,
        int maxInFlightMessages,
        int maxFetchRecords,
        Persister persister,
        GroupConfigManager groupConfigManager,
        ShareGroupMetrics shareGroupMetrics,
        BrokerTopicStats brokerTopicStats
    ) {
        this(replicaManager,
            time,
            cache,
            partitionCacheMap,
            defaultRecordLockDurationMs,
            new SystemTimerReaper("share-group-lock-timeout-reaper",
                new SystemTimer("share-group-lock-timeout")),
            maxDeliveryCount,
            maxInFlightMessages,
            maxFetchRecords,
            persister,
            groupConfigManager,
            shareGroupMetrics,
            brokerTopicStats
        );
    }

    // Visible for testing.
    SharePartitionManager(
            ReplicaManager replicaManager,
            Time time,
            ShareSessionCache cache,
            Map<SharePartitionKey, SharePartition> partitionCacheMap,
            int defaultRecordLockDurationMs,
            Timer timer,
            int maxDeliveryCount,
            int maxInFlightMessages,
            int maxFetchRecords,
            Persister persister,
            GroupConfigManager groupConfigManager,
            ShareGroupMetrics shareGroupMetrics,
            BrokerTopicStats brokerTopicStats
    ) {
        this.replicaManager = replicaManager;
        this.time = time;
        this.cache = cache;
        this.partitionCacheMap = partitionCacheMap;
        this.defaultRecordLockDurationMs = defaultRecordLockDurationMs;
        this.timer = timer;
        this.maxDeliveryCount = maxDeliveryCount;
        this.maxInFlightMessages = maxInFlightMessages;
        this.persister = persister;
        this.groupConfigManager = groupConfigManager;
        this.shareGroupMetrics = shareGroupMetrics;
        this.maxFetchRecords = maxFetchRecords;
        this.brokerTopicStats = brokerTopicStats;
    }

    /**
     * The fetch messages method is used to fetch messages from the log for the specified topic-partitions.
     * The method returns a future that will be completed with the fetched messages.
     *
     * @param groupId The group id, this is used to identify the share group.
     * @param memberId The member id, generated by the group-coordinator, this is used to identify the client.
     * @param fetchParams The fetch parameters from the share fetch request.
     * @param batchSize The number of records per acquired records batch.
     * @param partitionMaxBytes The maximum number of bytes to fetch for each partition.
     *
     * @return A future that will be completed with the fetched messages.
     */
    public CompletableFuture<Map<TopicIdPartition, PartitionData>> fetchMessages(
        String groupId,
        String memberId,
        FetchParams fetchParams,
        int sessionEpoch,
        int batchSize,
        LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes
    ) {
        log.trace("Fetch request for topicIdPartitions: {} with groupId: {} fetch params: {}",
                partitionMaxBytes.keySet(), groupId, fetchParams);

        LinkedHashMap<TopicIdPartition, Integer> topicIdPartitions = PartitionRotateStrategy
            .type(PartitionRotateStrategy.StrategyType.ROUND_ROBIN)
            .rotate(partitionMaxBytes, new PartitionRotateMetadata(sessionEpoch));

        CompletableFuture<Map<TopicIdPartition, PartitionData>> future = new CompletableFuture<>();
        processShareFetch(new ShareFetch(fetchParams, groupId, memberId, future, topicIdPartitions, batchSize, maxFetchRecords, brokerTopicStats));

        return future;
    }

    /**
     * The acknowledge method is used to acknowledge the messages that have been fetched.
     * The method returns a future that will be completed with the acknowledge response.
     *
     * @param memberId The member id, generated by the group-coordinator, this is used to identify the client.
     * @param groupId The group id, this is used to identify the share group.
     * @param acknowledgeTopics The acknowledge topics and their corresponding acknowledge batches.
     *
     * @return A future that will be completed with the acknowledge response.
     */
    public CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> acknowledge(
        String memberId,
        String groupId,
        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics
    ) {
        log.trace("Acknowledge request for topicIdPartitions: {} with groupId: {}",
            acknowledgeTopics.keySet(), groupId);
        Map<TopicIdPartition, CompletableFuture<Throwable>> futures = new HashMap<>();
        // Track the topics for which we have received an acknowledgement for metrics.
        Set<String> topics = new HashSet<>();
        acknowledgeTopics.forEach((topicIdPartition, acknowledgePartitionBatches) -> {
            topics.add(topicIdPartition.topic());
            SharePartitionKey sharePartitionKey = sharePartitionKey(groupId, topicIdPartition);
            SharePartition sharePartition = partitionCacheMap.get(sharePartitionKey);
            if (sharePartition != null) {
                CompletableFuture<Throwable> future = new CompletableFuture<>();
                sharePartition.acknowledge(memberId, acknowledgePartitionBatches).whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        fencedSharePartitionHandler().accept(sharePartitionKey, throwable);
                        future.complete(throwable);
                        return;
                    }
                    acknowledgePartitionBatches.forEach(batch -> {
                        // Client can either send a single entry in acknowledgeTypes which represents
                        // the state of the complete batch or can send individual offsets state.
                        if (batch.acknowledgeTypes().size() == 1) {
                            shareGroupMetrics.recordAcknowledgement(batch.acknowledgeTypes().get(0), batch.lastOffset() - batch.firstOffset() + 1);
                        } else {
                            batch.acknowledgeTypes().forEach(shareGroupMetrics::recordAcknowledgement);
                        }
                    });
                    future.complete(null);
                });

                // If we have an acknowledgement completed for a topic-partition, then we should check if
                // there is a pending share fetch request for the topic-partition and complete it.
                DelayedShareFetchKey delayedShareFetchKey = new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition());
                replicaManager.completeDelayedShareFetchRequest(delayedShareFetchKey);

                futures.put(topicIdPartition, future);
            } else {
                futures.put(topicIdPartition, CompletableFuture.completedFuture(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception()));
            }
        });

        // Update the metrics for the topics for which we have received an acknowledgement.
        topics.forEach(topic -> {
            brokerTopicStats.allTopicsStats().totalShareAcknowledgementRequestRate().mark();
            brokerTopicStats.topicStats(topic).totalShareAcknowledgementRequestRate().mark();
        });

        return mapAcknowledgementFutures(futures, Optional.of(failedShareAcknowledgeMetricsHandler()));
    }

    /**
     * The release session method is used to release the session for the memberId of respective group.
     * The method post removing session also releases acquired records for the respective member.
     * The method returns a future that will be completed with the release response.
     *
     * @param groupId The group id, this is used to identify the share group.
     * @param memberId The member id, generated by the group-coordinator, this is used to identify the client.
     *
     * @return A future that will be completed with the release response.
     */
    public CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> releaseSession(
        String groupId,
        String memberId
    ) {
        log.trace("Release session request for groupId: {}, memberId: {}", groupId, memberId);
        Uuid memberIdUuid = Uuid.fromString(memberId);
        List<TopicIdPartition> topicIdPartitions = cachedTopicIdPartitionsInShareSession(
            groupId, memberIdUuid);
        // Remove the share session from the cache.
        ShareSessionKey key = shareSessionKey(groupId, memberIdUuid);
        if (cache.remove(key) == null) {
            log.error("Share session error for {}: no such share session found", key);
            return FutureUtils.failedFuture(Errors.SHARE_SESSION_NOT_FOUND.exception());
        } else {
            log.debug("Removed share session with key " + key);
        }

        // Additionally release the acquired records for the respective member.
        if (topicIdPartitions.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        Map<TopicIdPartition, CompletableFuture<Throwable>> futuresMap = new HashMap<>();
        topicIdPartitions.forEach(topicIdPartition -> {
            SharePartitionKey sharePartitionKey = sharePartitionKey(groupId, topicIdPartition);
            SharePartition sharePartition = partitionCacheMap.get(sharePartitionKey);
            if (sharePartition == null) {
                log.error("No share partition found for groupId {} topicPartition {} while releasing acquired topic partitions", groupId, topicIdPartition);
                futuresMap.put(topicIdPartition, CompletableFuture.completedFuture(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception()));
            } else {
                CompletableFuture<Throwable> future = new CompletableFuture<>();
                sharePartition.releaseAcquiredRecords(memberId).whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        fencedSharePartitionHandler().accept(sharePartitionKey, throwable);
                        future.complete(throwable);
                        return;
                    }
                    future.complete(null);
                });
                // If we have a release acquired request completed for a topic-partition, then we should check if
                // there is a pending share fetch request for the topic-partition and complete it.
                DelayedShareFetchKey delayedShareFetchKey = new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition());
                replicaManager.completeDelayedShareFetchRequest(delayedShareFetchKey);

                futuresMap.put(topicIdPartition, future);
            }
        });

        return mapAcknowledgementFutures(futuresMap, Optional.empty());
    }

    private CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> mapAcknowledgementFutures(
        Map<TopicIdPartition, CompletableFuture<Throwable>> futuresMap,
        Optional<Consumer<Set<String>>> failedMetricsHandler
    ) {
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
            futuresMap.values().toArray(new CompletableFuture<?>[0]));
        return allFutures.thenApply(v -> {
            Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = new HashMap<>();
            // Keep the set as same topic might appear multiple times. Multiple partitions can fail for same topic.
            Set<String> failedTopics = new HashSet<>();
            futuresMap.forEach((topicIdPartition, future) -> {
                ShareAcknowledgeResponseData.PartitionData partitionData = new ShareAcknowledgeResponseData.PartitionData()
                    .setPartitionIndex(topicIdPartition.partition());
                Throwable t = future.join();
                if (t != null) {
                    partitionData.setErrorCode(Errors.forException(t).code())
                        .setErrorMessage(t.getMessage());
                    failedTopics.add(topicIdPartition.topic());
                }
                result.put(topicIdPartition, partitionData);
            });
            failedMetricsHandler.ifPresent(handler -> handler.accept(failedTopics));
            return result;
        });
    }

    /**
     * The newContext method is used to create a new share fetch context for every share fetch request.
     * @param groupId The group id in the share fetch request.
     * @param shareFetchData The topic-partitions and their corresponding maxBytes data in the share fetch request.
     * @param toForget The topic-partitions to forget present in the share fetch request.
     * @param reqMetadata The metadata in the share fetch request.
     * @param isAcknowledgeDataPresent This tells whether the fetch request received includes piggybacked acknowledgements or not
     * @return The new share fetch context object
     */
    public ShareFetchContext newContext(String groupId, Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData,
                                        List<TopicIdPartition> toForget, ShareRequestMetadata reqMetadata, Boolean isAcknowledgeDataPresent) {
        ShareFetchContext context;
        // TopicPartition with maxBytes as 0 should not be added in the cachedPartitions
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchDataWithMaxBytes = new HashMap<>();
        shareFetchData.forEach((tp, sharePartitionData) -> {
            if (sharePartitionData.maxBytes > 0) shareFetchDataWithMaxBytes.put(tp, sharePartitionData);
        });
        // If the request's epoch is FINAL_EPOCH or INITIAL_EPOCH, we should remove the existing sessions. Also, start a
        // new session in case it is INITIAL_EPOCH. Hence, we need to treat them as special cases.
        if (reqMetadata.isFull()) {
            ShareSessionKey key = shareSessionKey(groupId, reqMetadata.memberId());
            if (reqMetadata.epoch() == ShareRequestMetadata.FINAL_EPOCH) {
                // If the epoch is FINAL_EPOCH, don't try to create a new session.
                if (!shareFetchDataWithMaxBytes.isEmpty()) {
                    throw Errors.INVALID_REQUEST.exception();
                }
                if (cache.get(key) == null) {
                    log.error("Share session error for {}: no such share session found", key);
                    throw Errors.SHARE_SESSION_NOT_FOUND.exception();
                }
                context = new FinalContext();
            } else {
                if (isAcknowledgeDataPresent) {
                    log.error("Acknowledge data present in Initial Fetch Request for group {} member {}", groupId, reqMetadata.memberId());
                    throw Errors.INVALID_REQUEST.exception();
                }
                if (cache.remove(key) != null) {
                    log.debug("Removed share session with key {}", key);
                }
                ImplicitLinkedHashCollection<CachedSharePartition> cachedSharePartitions = new
                        ImplicitLinkedHashCollection<>(shareFetchDataWithMaxBytes.size());
                shareFetchDataWithMaxBytes.forEach((topicIdPartition, reqData) ->
                    cachedSharePartitions.mustAdd(new CachedSharePartition(topicIdPartition, reqData, false)));
                ShareSessionKey responseShareSessionKey = cache.maybeCreateSession(groupId, reqMetadata.memberId(),
                        time.milliseconds(), cachedSharePartitions);
                if (responseShareSessionKey == null) {
                    log.error("Could not create a share session for group {} member {}", groupId, reqMetadata.memberId());
                    throw Errors.SHARE_SESSION_NOT_FOUND.exception();
                }

                context = new ShareSessionContext(reqMetadata, shareFetchDataWithMaxBytes);
                log.debug("Created a new ShareSessionContext with key {} isSubsequent {} returning {}. A new share " +
                        "session will be started.", responseShareSessionKey, false,
                        partitionsToLogString(shareFetchDataWithMaxBytes.keySet()));
            }
        } else {
            // We update the already existing share session.
            synchronized (cache) {
                ShareSessionKey key = shareSessionKey(groupId, reqMetadata.memberId());
                ShareSession shareSession = cache.get(key);
                if (shareSession == null) {
                    log.error("Share session error for {}: no such share session found", key);
                    throw Errors.SHARE_SESSION_NOT_FOUND.exception();
                }
                if (shareSession.epoch != reqMetadata.epoch()) {
                    log.debug("Share session error for {}: expected epoch {}, but got {} instead", key,
                            shareSession.epoch, reqMetadata.epoch());
                    throw Errors.INVALID_SHARE_SESSION_EPOCH.exception();
                }
                Map<ShareSession.ModifiedTopicIdPartitionType, List<TopicIdPartition>> modifiedTopicIdPartitions = shareSession.update(
                        shareFetchDataWithMaxBytes, toForget);
                cache.touch(shareSession, time.milliseconds());
                shareSession.epoch = ShareRequestMetadata.nextEpoch(shareSession.epoch);
                log.debug("Created a new ShareSessionContext for session key {}, epoch {}: " +
                                "added {}, updated {}, removed {}", shareSession.key(), shareSession.epoch,
                        partitionsToLogString(modifiedTopicIdPartitions.get(
                                ShareSession.ModifiedTopicIdPartitionType.ADDED)),
                        partitionsToLogString(modifiedTopicIdPartitions.get(ShareSession.ModifiedTopicIdPartitionType.UPDATED)),
                        partitionsToLogString(modifiedTopicIdPartitions.get(ShareSession.ModifiedTopicIdPartitionType.REMOVED))
                );
                context = new ShareSessionContext(reqMetadata, shareSession);
            }
        }
        return context;
    }

    /**
     * The acknowledgeSessionUpdate method is used to update the request epoch and lastUsed time of the share session.
     * @param groupId The group id in the share fetch request.
     * @param reqMetadata The metadata in the share acknowledge request.
     */
    public void acknowledgeSessionUpdate(String groupId, ShareRequestMetadata reqMetadata) {
        if (reqMetadata.epoch() == ShareRequestMetadata.INITIAL_EPOCH) {
            // ShareAcknowledge Request cannot have epoch as INITIAL_EPOCH (0)
            throw Errors.INVALID_SHARE_SESSION_EPOCH.exception();
        } else {
            synchronized (cache) {
                ShareSessionKey key = shareSessionKey(groupId, reqMetadata.memberId());
                ShareSession shareSession = cache.get(key);
                if (shareSession == null) {
                    log.debug("Share session error for {}: no such share session found", key);
                    throw Errors.SHARE_SESSION_NOT_FOUND.exception();
                }

                if (reqMetadata.epoch() == ShareRequestMetadata.FINAL_EPOCH) {
                    // If the epoch is FINAL_EPOCH, then return. Do not update the cache.
                    return;
                } else if (shareSession.epoch != reqMetadata.epoch()) {
                    log.debug("Share session error for {}: expected epoch {}, but got {} instead", key,
                            shareSession.epoch, reqMetadata.epoch());
                    throw Errors.INVALID_SHARE_SESSION_EPOCH.exception();
                }
                cache.touch(shareSession, time.milliseconds());
                shareSession.epoch = ShareRequestMetadata.nextEpoch(shareSession.epoch);
            }
        }
    }

    /**
     * The cachedTopicIdPartitionsInShareSession method is used to get the cached topic-partitions in the share session.
     *
     * @param groupId The group id in the share fetch request.
     * @param memberId The member id in the share fetch request.
     *
     * @return The list of cached topic-partitions in the share session if present, otherwise an empty list.
     */
    // Visible for testing.
    List<TopicIdPartition> cachedTopicIdPartitionsInShareSession(String groupId, Uuid memberId) {
        ShareSessionKey key = shareSessionKey(groupId, memberId);
        ShareSession shareSession = cache.get(key);
        if (shareSession == null) {
            return Collections.emptyList();
        }
        List<TopicIdPartition> cachedTopicIdPartitions = new ArrayList<>();
        shareSession.partitionMap().forEach(cachedSharePartition -> cachedTopicIdPartitions.add(
            new TopicIdPartition(cachedSharePartition.topicId(), new TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()
            ))));
        return cachedTopicIdPartitions;
    }

    // Add the share fetch request to the delayed share fetch purgatory to process the fetch request if it can be
    // completed else watch until it can be completed/timeout.
    private void addDelayedShareFetch(DelayedShareFetch delayedShareFetch, List<DelayedShareFetchKey> keys) {
        replicaManager.addDelayedShareFetchRequest(delayedShareFetch, keys);
    }

    @Override
    public void close() throws Exception {
        this.timer.close();
        this.shareGroupMetrics.close();
    }

    private ShareSessionKey shareSessionKey(String groupId, Uuid memberId) {
        return new ShareSessionKey(groupId, memberId);
    }

    private static String partitionsToLogString(Collection<TopicIdPartition> partitions) {
        return ShareSession.partitionsToLogString(partitions, log.isTraceEnabled());
    }

    // Visible for testing.
    void processShareFetch(ShareFetch shareFetch) {
        if (shareFetch.partitionMaxBytes().isEmpty()) {
            // If there are no partitions to fetch then complete the future with an empty map.
            shareFetch.maybeComplete(Collections.emptyMap());
            return;
        }

        List<DelayedShareFetchKey> delayedShareFetchWatchKeys = new ArrayList<>();
        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        // Track the topics for which we have received a share fetch request for metrics.
        Set<String> topics = new HashSet<>();
        for (TopicIdPartition topicIdPartition : shareFetch.partitionMaxBytes().keySet()) {
            topics.add(topicIdPartition.topic());
            SharePartitionKey sharePartitionKey = sharePartitionKey(
                shareFetch.groupId(),
                topicIdPartition
            );

            SharePartition sharePartition;
            try {
                sharePartition = getOrCreateSharePartition(sharePartitionKey);
            } catch (Exception e) {
                log.debug("Error processing share fetch request", e);
                shareFetch.addErroneous(topicIdPartition, e);
                // Continue iteration for other partitions in the request.
                continue;
            }

            // We add a key corresponding to each share partition in the request in the group so that when there are
            // acknowledgements/acquisition lock timeout etc., we have a way to perform checkAndComplete for all
            // such requests which are delayed because of lack of data to acquire for the share partition.
            DelayedShareFetchKey delayedShareFetchKey = new DelayedShareFetchGroupKey(shareFetch.groupId(),
                topicIdPartition.topicId(), topicIdPartition.partition());
            delayedShareFetchWatchKeys.add(delayedShareFetchKey);
            // We add a key corresponding to each topic partition in the request so that when the HWM is updated
            // for any topic partition, we have a way to perform checkAndComplete for all such requests which are
            // delayed because of lack of data to acquire for the topic partition.
            delayedShareFetchWatchKeys.add(new DelayedShareFetchPartitionKey(topicIdPartition.topicId(), topicIdPartition.partition()));

            CompletableFuture<Void> initializationFuture = sharePartition.maybeInitialize();
            final boolean initialized = initializationFuture.isDone();
            initializationFuture.whenComplete((result, throwable) -> {
                if (throwable != null) {
                    handleInitializationException(sharePartitionKey, shareFetch, throwable);
                }
                // Though the share partition is initialized asynchronously, but if already initialized or
                // errored then future should be completed immediately. If the initialization is not completed
                // immediately then the requests might be waiting in purgatory until the share partition
                // is initialized. Hence, trigger the completion of all pending delayed share fetch requests
                // for the share partition.
                if (!initialized) {
                    shareGroupMetrics.partitionLoadTime(sharePartition.loadStartTimeMs());
                    replicaManager.completeDelayedShareFetchRequest(delayedShareFetchKey);
                }
            });
            sharePartitions.put(topicIdPartition, sharePartition);
        }

        // Update the metrics for the topics for which we have received a share fetch request.
        topics.forEach(topic -> {
            brokerTopicStats.allTopicsStats().totalShareFetchRequestRate().mark();
            brokerTopicStats.topicStats(topic).totalShareFetchRequestRate().mark();
        });

        // If all the partitions in the request errored out, then complete the fetch request with an exception.
        if (shareFetch.errorInAllPartitions()) {
            shareFetch.maybeComplete(Collections.emptyMap());
            // Do not proceed with share fetch processing as all the partitions errored out.
            return;
        }

        // Add the share fetch to the delayed share fetch purgatory to process the fetch request.
        // The request will be added irrespective of whether the share partition is initialized or not.
        // Once the share partition is initialized, the delayed share fetch will be completed.
        addDelayedShareFetch(new DelayedShareFetch(shareFetch, replicaManager, fencedSharePartitionHandler(), sharePartitions, shareGroupMetrics, time), delayedShareFetchWatchKeys);
    }

    private SharePartition getOrCreateSharePartition(SharePartitionKey sharePartitionKey) {
        return partitionCacheMap.computeIfAbsent(sharePartitionKey,
                k -> {
                    int leaderEpoch = ShareFetchUtils.leaderEpoch(replicaManager, sharePartitionKey.topicIdPartition().topicPartition());
                    // Attach listener to Partition which shall invoke partition change handlers.
                    // However, as there could be multiple share partitions (per group name) for a single topic-partition,
                    // hence create separate listeners per share partition which holds the share partition key
                    // to identify the respective share partition.
                    SharePartitionListener listener = new SharePartitionListener(sharePartitionKey, replicaManager, partitionCacheMap);
                    replicaManager.maybeAddListener(sharePartitionKey.topicIdPartition().topicPartition(), listener);
                    SharePartition partition = new SharePartition(
                            sharePartitionKey.groupId(),
                            sharePartitionKey.topicIdPartition(),
                            leaderEpoch,
                            maxInFlightMessages,
                            maxDeliveryCount,
                            defaultRecordLockDurationMs,
                            timer,
                            time,
                            persister,
                            replicaManager,
                            groupConfigManager,
                            listener
                    );
                    return partition;
                });
    }

    private void handleInitializationException(
            SharePartitionKey sharePartitionKey,
            ShareFetch shareFetch,
            Throwable throwable) {
        if (throwable instanceof LeaderNotAvailableException) {
            log.debug("The share partition with key {} is not initialized yet", sharePartitionKey);
            // Skip any handling for this error as the share partition is still loading. The request
            // to fetch will be added in purgatory and will be completed once either timed out
            // or the share partition initialization completes.
            return;
        }

        // Remove the partition from the cache as it's failed to initialize.
        removeSharePartitionFromCache(sharePartitionKey, partitionCacheMap, replicaManager);
        // The partition initialization failed, so add the partition to the erroneous partitions.
        log.debug("Error initializing share partition with key {}", sharePartitionKey, throwable);
        shareFetch.addErroneous(sharePartitionKey.topicIdPartition(), throwable);
    }

    /**
     * The method returns a BiConsumer that handles share partition exceptions. The BiConsumer accepts
     * a share partition key and a throwable which specifies the exception.
     *
     * @return A BiConsumer that handles share partition exceptions.
     */
    private BiConsumer<SharePartitionKey, Throwable> fencedSharePartitionHandler() {
        return (sharePartitionKey, throwable) -> {
            if (throwable instanceof NotLeaderOrFollowerException || throwable instanceof FencedStateEpochException ||
                throwable instanceof GroupIdNotFoundException || throwable instanceof UnknownTopicOrPartitionException) {
                log.info("The share partition with key {} is fenced: {}", sharePartitionKey, throwable.getMessage());
                // The share partition is fenced hence remove the partition from map and let the client retry.
                // But surface the error to the client so client might take some action i.e. re-fetch
                // the metadata and retry the fetch on new leader.
                removeSharePartitionFromCache(sharePartitionKey, partitionCacheMap, replicaManager);
            }
        };
    }

    private SharePartitionKey sharePartitionKey(String groupId, TopicIdPartition topicIdPartition) {
        return new SharePartitionKey(groupId, topicIdPartition);
    }

    private static void removeSharePartitionFromCache(
        SharePartitionKey sharePartitionKey,
        Map<SharePartitionKey, SharePartition> map,
        ReplicaManager replicaManager
    ) {
        SharePartition sharePartition = map.remove(sharePartitionKey);
        if (sharePartition != null) {
            sharePartition.markFenced();
            replicaManager.removeListener(sharePartitionKey.topicIdPartition().topicPartition(), sharePartition.listener());
        }
    }

    /**
     * The handler to update the failed share acknowledge request metrics.
     *
     * @return A Consumer that updates the failed share acknowledge request metrics.
     */
    private Consumer<Set<String>> failedShareAcknowledgeMetricsHandler() {
        return failedTopics -> {
            // Update failed share acknowledge request metric.
            failedTopics.forEach(topic -> {
                brokerTopicStats.allTopicsStats().failedShareAcknowledgementRequestRate().mark();
                brokerTopicStats.topicStats(topic).failedShareAcknowledgementRequestRate().mark();
            });
        };
    }

    /**
     * The SharePartitionListener is used to listen for partition events. The share partition is associated with
     * the topic-partition, we need to handle the partition events for the share partition.
     * <p>
     * The partition cache map stores share partitions against share partition key which comprises
     * group and topic-partition. Instead of maintaining a separate map for topic-partition to share partitions,
     * we can maintain the share partition key in the listener and create a new listener for each share partition.
     */
    static class SharePartitionListener implements PartitionListener {

        private final SharePartitionKey sharePartitionKey;
        private final ReplicaManager replicaManager;
        private final Map<SharePartitionKey, SharePartition> partitionCacheMap;

        SharePartitionListener(
            SharePartitionKey sharePartitionKey,
            ReplicaManager replicaManager,
            Map<SharePartitionKey, SharePartition> partitionCacheMap
        ) {
            this.sharePartitionKey = sharePartitionKey;
            this.replicaManager = replicaManager;
            this.partitionCacheMap = partitionCacheMap;
        }

        @Override
        public void onFailed(TopicPartition topicPartition) {
            log.debug("The share partition failed listener is invoked for the topic-partition: {}, share-partition: {}",
                topicPartition, sharePartitionKey);
            onUpdate(topicPartition);
        }

        @Override
        public void onDeleted(TopicPartition topicPartition) {
            log.debug("The share partition delete listener is invoked for the topic-partition: {}, share-partition: {}",
                topicPartition, sharePartitionKey);
            onUpdate(topicPartition);
        }

        @Override
        public void onBecomingFollower(TopicPartition topicPartition) {
            log.debug("The share partition becoming follower listener is invoked for the topic-partition: {}, share-partition: {}",
                topicPartition, sharePartitionKey);
            onUpdate(topicPartition);
        }

        private void onUpdate(TopicPartition topicPartition) {
            if (!sharePartitionKey.topicIdPartition().topicPartition().equals(topicPartition)) {
                log.error("The share partition listener is invoked for the wrong topic-partition: {}, share-partition: {}",
                    topicPartition, sharePartitionKey);
                return;
            }
            removeSharePartitionFromCache(sharePartitionKey, partitionCacheMap, replicaManager);
        }
    }
}
