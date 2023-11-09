/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.common.Closure;
import io.openmessaging.storage.dledger.common.ShutdownAbleThread;
import io.openmessaging.storage.dledger.common.Status;
import io.openmessaging.storage.dledger.common.TimeoutFuture;
import io.openmessaging.storage.dledger.common.WriteClosure;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.metrics.DLedgerMetricsManager;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.InstallSnapshotRequest;
import io.openmessaging.storage.dledger.protocol.InstallSnapshotResponse;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
import io.openmessaging.storage.dledger.snapshot.DownloadSnapshot;
import io.openmessaging.storage.dledger.snapshot.SnapshotManager;
import io.openmessaging.storage.dledger.snapshot.SnapshotMeta;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.statemachine.ApplyEntry;
import io.openmessaging.storage.dledger.statemachine.StateMachineCaller;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import io.openmessaging.storage.dledger.utils.Pair;
import io.openmessaging.storage.dledger.utils.PreConditions;
import io.openmessaging.storage.dledger.utils.Quota;

import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.LABEL_REMOTE_ID;

public class DLedgerEntryPusher {

    private static final Logger LOGGER = LoggerFactory.getLogger(DLedgerEntryPusher.class);

    private final DLedgerConfig dLedgerConfig;
    private final DLedgerStore dLedgerStore;

    private final MemberState memberState;

    private final DLedgerRpcService dLedgerRpcService;

    /**
     * peerWaterMarksByTerm中记录了日志转发的进度，KEY为Term，VALUE为ConcurrentMap，ConcurrentMap中的KEY为Follower节点的ID（peerId）
     * ，VALUE为该节点已经同步完毕的最新的那条消息的index：
     * 记录Follower节点的同步进度，KEY为Term，VALUE为ConcurrentMap
     * ConcurrentMap中的KEY为Follower节点的ID（peerId），VALUE为该节点已经同步完毕的最新的那条消息的index
     */
    private final Map<Long/*term*/, ConcurrentMap<String/*peer id*/, Long/*match index*/>> peerWaterMarksByTerm = new ConcurrentHashMap<>();

    /**
     * 1. 外层的KEY为Term的值，value是一个ConcurrentMap
     * ConcurrentMap的KEY为消息的index，value为此条消息写入请求的异步响应对象AppendEntryResponse
     *
     * 2. pendingAppendResponsesByTerm的值是在什么时候加入的？
     *      在写入Leader节点之后，调用DLedgerEntryPusher的waitAck方法（后面会讲到）的时候，如果集群中有多个节点，会为当前的请求创建AppendFuture<AppendEntryResponse>响应对象加入到pendingAppendResponsesByTerm中
     *      ，所以可以通过pendingAppendResponsesByTerm中存放的响应对象数量判断当前Term有多少个在等待的写入请求。
     *
     */
    private final Map<Long/*term*/, ConcurrentMap<Long/*index*/, Closure/*upper callback*/>> pendingClosure = new ConcurrentHashMap<>();

    private final EntryHandler entryHandler;

    private final QuorumAckChecker quorumAckChecker;

    private final Map<String/*peer id*/, EntryDispatcher/*entry dispatcher for each peer*/> dispatcherMap = new HashMap<>();

    private final String selfId;

    private StateMachineCaller fsmCaller;

    public DLedgerEntryPusher(DLedgerConfig dLedgerConfig, MemberState memberState, DLedgerStore dLedgerStore,
        DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.selfId = this.dLedgerConfig.getSelfId();
        this.memberState = memberState;
        this.dLedgerStore = dLedgerStore;
        this.dLedgerRpcService = dLedgerRpcService;
        for (String peer : memberState.getPeerMap().keySet()) {
            if (!peer.equals(memberState.getSelfId())) {
                // 为集群中除当前节点以外的其他节点创建EntryDispatcher
                dispatcherMap.put(peer, new EntryDispatcher(peer, LOGGER));
            }
        }
        // 创建EntryHandler
        this.entryHandler = new EntryHandler(LOGGER);
        // 创建QuorumAckChecker
        this.quorumAckChecker = new QuorumAckChecker(LOGGER);
    }

    /**
     * 消息写入Leader之后，Leader节点会将消息转发给其他Follower节点，这个过程是异步进行处理的，接下来看下消息的复制过程。
     * 1. 在DLedgerEntryPusher的startup方法中会启动以下线程：
     *  1. EntryDispatcher：用于Leader节点向Follwer节点转发日志；
     *  2. EntryHandler：用于Follower节点处理Leader节点发送的日志；
     *  3. QuorumAckChecker：用于Leader节点等待Follower节点同步；
     * 需要注意的是，Leader节点会为每个Follower节点创建EntryDispatcher转发器，每一个EntryDispatcher负责一个节点的日志转发，多个节点之间是并行处理的。
     */
    public void startup() {
        // 启动EntryHandler
        entryHandler.start();
        // 启动QuorumAckChecker
        quorumAckChecker.start();
        // 启动EntryDispatcher
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.start();
        }
    }

    public void shutdown() {
        entryHandler.shutdown();
        quorumAckChecker.shutdown();
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.shutdown();
        }
    }

    public void registerStateMachine(final StateMachineCaller fsmCaller) {
        this.fsmCaller = fsmCaller;
    }

    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return entryHandler.handlePush(request);
    }

    public CompletableFuture<InstallSnapshotResponse> handleInstallSnapshot(InstallSnapshotRequest request) {
        return entryHandler.handleInstallSnapshot(request);
    }

    private void checkTermForWaterMark(long term, String env) {
        // 如果peerWaterMarksByTerm不存在
        if (!peerWaterMarksByTerm.containsKey(term)) {
            LOGGER.info("Initialize the watermark in {} for term={}", env, term);
            // 创建ConcurrentMap
            ConcurrentMap<String, Long> waterMarks = new ConcurrentHashMap<>();
            // 对集群中的节点进行遍历
            for (String peer : memberState.getPeerMap().keySet()) {
                // 初始化，KEY为节点的PEER,VALUE为-1
                waterMarks.put(peer, -1L);
            }
            // 加入到peerWaterMarksByTerm
            peerWaterMarksByTerm.putIfAbsent(term, waterMarks);
        }
    }

    private void checkTermForPendingMap(long term, String env) {
        // 如果pendingAppendResponsesByTerm不包含
        if (!pendingClosure.containsKey(term)) {
            LOGGER.info("Initialize the pending closure map in {} for term={}", env, term);
            // 创建一个ConcurrentHashMap加入到pendingAppendResponsesByTerm
            pendingClosure.putIfAbsent(term, new ConcurrentHashMap<>());
        }

    }

    /**
     * 更新水位线
     * @param term
     * @param peerId
     * @param index
     */
    private void updatePeerWaterMark(long term, String peerId, long index) {
        synchronized (peerWaterMarksByTerm) {
            // 校验
            checkTermForWaterMark(term, "updatePeerWaterMark");
            // 如果之前的水位线小于当前的index进行更新
            if (peerWaterMarksByTerm.get(term).get(peerId) < index) {
                peerWaterMarksByTerm.get(term).put(peerId, index);
            }
        }
    }

    /**
     * 获取节点的同步进度
     * 调用getPeerWaterMark方法的时候，首先会调用checkTermForWaterMark检查peerWaterMarksByTerm是否存在数据，如果不存在， 创建ConcurrentMap
     * ，并遍历集群中的节点，加入到ConcurrentMap，其中KEY为节点的ID，value为默认值-1，当消息成功写入Follower节点后，会调用updatePeerWaterMark更同步进度：
     * @param term
     * @param peerId
     * @return
     */
    public long getPeerWaterMark(long term, String peerId) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "getPeerWaterMark");
            return peerWaterMarksByTerm.get(term).get(peerId);
        }
    }

    /**
     * 调用isPendingFull方法的时候，会先校验当前Term是否在pendingAppendResponsesByTerm中有对应的值，如果没有
     * ，创建一个ConcurrentHashMap进行初始化，否则获取对应的ConcurrentHashMap里面数据的个数，与MaxPendingRequestsNum做对比，校验是否超过了最大值
     * @param currTerm
     * @return
     */
    public boolean isPendingFull(long currTerm) {
        // 校验currTerm是否在pendingAppendResponsesByTerm中
        checkTermForPendingMap(currTerm, "isPendingFull");
        // 判断当前Term对应的写入请求数量是否超过了最大值
        return pendingClosure.get(currTerm).size() > dLedgerConfig.getMaxPendingRequestsNum();
    }

    public void appendClosure(Closure closure, long term, long index) {
        updatePeerWaterMark(term, memberState.getSelfId(), index);
        checkTermForPendingMap(term, "waitAck");
        Closure old = this.pendingClosure.get(term).put(index, closure);
        if (old != null) {
            LOGGER.warn("[MONITOR] get old wait at term = {}, index= {}", term, index);
        }
    }

    public void wakeUpDispatchers() {
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.wakeup();
        }
    }

    /**
     * Complete the TimeoutFuture in pendingAppendResponsesByTerm (CurrentTerm, index).
     * Called by statemachineCaller when a committed entry (CurrentTerm, index) was applying to statemachine done.
     *
     * @param task committed entry
     * @return true if complete success
     */
    public boolean completeResponseFuture(final ApplyEntry task) {
        final long index = task.getEntry().getIndex();
        final long term = this.memberState.currTerm();
        ConcurrentMap<Long, Closure> closureMap = this.pendingClosure.get(term);
        if (closureMap != null) {
            Closure closure = closureMap.remove(index);
            if (closure != null) {
                if (closure instanceof WriteClosure) {
                    WriteClosure writeClosure = (WriteClosure) closure;
                    writeClosure.setResp(task.getResp());
                }
                closure.done(Status.ok());
                LOGGER.info("Complete closure, term = {}, index = {}", term, index);
                return true;
            }
        }
        return false;
    }

    /**
     * Check responseFutures timeout from {beginIndex} in currentTerm
     *
     * @param beginIndex the beginning index to check
     */
    public void checkResponseFuturesTimeout(final long beginIndex) {
        final long term = this.memberState.currTerm();
        long maxIndex = this.memberState.getCommittedIndex() + dLedgerConfig.getMaxPendingRequestsNum() + 1;
        if (maxIndex > this.memberState.getLedgerEndIndex()) {
            maxIndex = this.memberState.getLedgerEndIndex() + 1;
        }
        ConcurrentMap<Long, Closure> closureMap = this.pendingClosure.get(term);
        if (closureMap != null && closureMap.size() > 0) {
            for (long i = beginIndex; i < maxIndex; i++) {
                Closure closure = closureMap.get(i);
                if (closure == null) {
                    // index may be removed for complete, we should continue scan
                } else if (closure.isTimeOut()) {
                    closure.done(Status.error(DLedgerResponseCode.WAIT_QUORUM_ACK_TIMEOUT));
                    closureMap.remove(i);
                } else {
                    break;
                }
            }
        }
    }

    /**
     * Check responseFutures elapsed before {endIndex} in currentTerm
     */
    private void checkResponseFuturesElapsed(final long endIndex) {
        final long currTerm = this.memberState.currTerm();
        final Map<Long, Closure> closureMap = this.pendingClosure.get(currTerm);
        for (Map.Entry<Long, Closure> closureEntry : closureMap.entrySet()) {
            if (closureEntry.getKey() <= endIndex) {
                closureEntry.getValue().done(Status.ok());
                closureMap.remove(closureEntry.getKey());
            }
        }
    }

    /**
     * This thread will check the quorum index and complete the pending requests.
     */
    private class QuorumAckChecker extends ShutdownAbleThread {

        private long lastPrintWatermarkTimeMs = System.currentTimeMillis();
        private long lastCheckLeakTimeMs = System.currentTimeMillis();
        private long lastCheckTimeoutTimeMs = System.currentTimeMillis();

        public QuorumAckChecker(Logger logger) {
            super("QuorumAckChecker-" + memberState.getSelfId(), logger);
        }

        /***
         * 1. QuorumAckChecker用于Leader节点等待Follower节点复制完毕，处理逻辑如下：
         *      1. 如果pendingAppendResponsesByTerm的个数大于1，对其进行遍历，如果KEY的值与当前Term不一致，说明数据已过期，将过期数据置为完成状态并从pendingAppendResponsesByTerm中移除；
         *      2. 如果peerWaterMarksByTerm个数大于1，对其进行遍历，同样找出与当前TERM不一致的数据，进行清理；
         *      3. 获取当前Term的peerWaterMarks，peerWaterMarks记录了每个Follower节点的日志复制进度，对所有的复制进度进行排序，取出处于中间位置的那个进度值，也就是消息的index值，这里不太好理解，举个例子，假如一个Leader有5个Follower节点，当前Term为1：
         *          {
         *             "1" : { // TERM的值，对应peerWaterMarks中的Key
         *              "节点1" : "1", // 节点1复制到第1条消息
         *              "节点2" : "1", // 节点2复制到第1条消息
         *              "节点3" : "2", // 节点3复制到第2条消息
         *              "节点4" : "3", // 节点4复制到第3条消息
         *              "节点5" : "3"  // 节点5复制到第3条消息
         *             }
         *          }
         *          对所有Follower节点的复制进度倒序排序之后的list如下：
         *          [3, 3, 2, 1, 1]
         *          取5 / 2 的整数部分为2，也就是下标为2处的值，对应节点3的复制进度（消息index为2），记录在quorumIndex变量中，节点4和5对应的消息进度大于消息2的
         *          ，所以对于消息2，集群已经有三个节点复制成功，满足了集群中大多数节点复制成功的条件。
         *              如果要判断某条消息是否集群中大多数节点已经成功写入，一种常规的处理方法，对每个节点的复制进度进行判断，记录已经复制成功的节点个数，这样需要每次遍历整个节点，效率比较低，所以这里RocketMQ使用了一种更高效的方式来判断某个消息是否获得了集群中大多数节点的响应。
         *      4. quorumIndex之前的消息都以成功复制，此时就可以更新提交点，调用updateCommittedIndex方法更新CommitterIndex的值；
         *      5. 处理处于quorumIndex和lastQuorumIndex（上次quorumIndex的值）之间的数据，比如上次lastQuorumIndex的值为1，本次quorumIndex为2，由于quorumIndex之前的消息已经获得了集群中大多数节点的响应，所以处于quorumIndex和lastQuorumIndex的数据需要清理，从pendingAppendResponsesByTerm中移除，并记录数量ackNum；
         *      6. 如果ackNum为0，表示quorumIndex与lastQuorumIndex相等,从quorumIndex + 1处开始，判断消息的写入请求是否已经超时，如果超时设置WAIT_QUORUM_ACK_TIMEOUT并返回响应；这一步主要是为了处理超时的请求；
         *      7. 如果上次校验时间超过1000ms或者needCheck为true，更新节点的复制进度，遍历当前term所有的请求响应，如果小于quorumIndex，将其设置成完成状态并移除响应，表示已完成，这一步主要是处理已经写入成功的消息对应的响应对象AppendEntryResponse，是否由于某些原因未移除，如果是需要进行清理；
         *      8. 更新lastQuorumIndex的值；
         *
         *
         *
         */
        @Override
        public void doWork() {
            try {
                if (DLedgerUtils.elapsed(lastPrintWatermarkTimeMs) > 3000) {
                    logger.info("[{}][{}] term={} ledgerBeforeBegin={} ledgerEnd={} committed={} watermarks={} appliedIndex={}",
                        memberState.getSelfId(), memberState.getRole(), memberState.currTerm(), dLedgerStore.getLedgerBeforeBeginIndex(), dLedgerStore.getLedgerEndIndex(), memberState.getCommittedIndex(), JSON.toJSONString(peerWaterMarksByTerm), memberState.getAppliedIndex());
                    lastPrintWatermarkTimeMs = System.currentTimeMillis();
                }
                // 获取当前的Term
                long currTerm = memberState.currTerm();
                checkTermForPendingMap(currTerm, "QuorumAckChecker");
                checkTermForWaterMark(currTerm, "QuorumAckChecker");
                // clear pending closure in old term
                // 如果pendingAppendResponsesByTerm的个数大于1
                if (pendingClosure.size() > 1) {
                    // 遍历，处理与当前TERM不一致的数据
                    for (Long term : pendingClosure.keySet()) {
                        // 如果与当前Term一致
                        if (term == currTerm) {
                            continue;
                        }
                        // 对VALUE进行遍历
                        for (Map.Entry<Long, Closure> futureEntry : pendingClosure.get(term).entrySet()) {
                            logger.info("[TermChange] Will clear the pending closure index={} for term changed from {} to {}", futureEntry.getKey(), term, currTerm);
                            futureEntry.getValue().done(Status.error(DLedgerResponseCode.EXPIRED_TERM));
                        }
                        // 移除
                        pendingClosure.remove(term);
                    }
                }
                // clear peer watermarks in old term
                // 处理与当前TERM不一致的数据
                if (peerWaterMarksByTerm.size() > 1) {
                    for (Long term : peerWaterMarksByTerm.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        logger.info("[TermChange] Will clear the watermarks for term changed from {} to {}", term, currTerm);
                        // 移除
                        peerWaterMarksByTerm.remove(term);
                    }
                }

                // clear the pending closure which index <= applyIndex
                if (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000) {
                    checkResponseFuturesElapsed(DLedgerEntryPusher.this.memberState.getAppliedIndex());
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                if (DLedgerUtils.elapsed(lastCheckTimeoutTimeMs) > 1000) {
                    // clear the timeout pending closure should check all since it can timeout for different index
                    checkResponseFuturesTimeout(DLedgerEntryPusher.this.memberState.getAppliedIndex() + 1);
                    lastCheckTimeoutTimeMs = System.currentTimeMillis();
                }
                // 如果不是Leader
                if (!memberState.isLeader()) {
                    waitForRunning(1);
                    return;
                }

                // update peer watermarks of self
                updatePeerWaterMark(currTerm, memberState.getSelfId(), dLedgerStore.getLedgerEndIndex());

                // calculate the median of watermarks(which we can ensure that more than half of the nodes have been pushed the corresponding entry)
                // we can also call it quorumIndex
                // 获取当前Term的peerWaterMarks，也就是每个Follower节点的复制进度
                Map<String, Long> peerWaterMarks = peerWaterMarksByTerm.get(currTerm);
                // 对value进行排序
                List<Long> sortedWaterMarks = peerWaterMarks.values()
                    .stream()
                    .sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
                // 取中位数
                long quorumIndex = sortedWaterMarks.get(sortedWaterMarks.size() / 2);

                // advance the commit index
                // we can only commit the index whose term is equals to current term (refer to raft paper 5.4.2)
                // 中位数之前的消息都已同步成功，此时更新CommittedIndex
                if (DLedgerEntryPusher.this.memberState.leaderUpdateCommittedIndex(currTerm, quorumIndex)) {
                    DLedgerEntryPusher.this.fsmCaller.onCommitted(quorumIndex);
                } else {
                    // If the commit index is not advanced, we should wait for the next round
                    waitForRunning(1);
                }
            } catch (Throwable t) {
                DLedgerEntryPusher.LOGGER.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
        }
    }

    /**
     * This thread will be activated by the leader.
     * This thread will push the entry to follower(identified by peerId) and update the completed pushed index to index map.
     * Should generate a single thread for each peer.
     * The push has 4 types:
     * APPEND : append the entries to the follower
     * COMPARE : if the leader changes, the new leader should compare its entries to follower's
     * TRUNCATE : if the leader finished comparing by an index, the leader will send a request to truncate the follower's ledger
     * COMMIT: usually, the leader will attach the committed index with the APPEND request, but if the append requests are few and scattered,
     * the leader will send a pure request to inform the follower of committed index.
     * <p>
     * The common transferring between these types are as following:
     * <p>
     * COMPARE ---- TRUNCATE ---- APPEND ---- COMMIT
     * ^                             |
     * |---<-----<------<-------<----|
     *
     *
     *
     * 用于Leader节点向Follower转发日志，它继承了ShutdownAbleThread，所以会启动线程处理日志转发(日志转发线程)，入口在doWork方法中。
     *
     *
     */
    private class EntryDispatcher extends ShutdownAbleThread {

        private final AtomicReference<EntryDispatcherState> type = new AtomicReference<>(EntryDispatcherState.COMPARE);
        private long lastPushCommitTimeMs = -1;
        private final String peerId;

        /**
         * the index of the next entry to push(initialized to the next of the last entry in the store)
         * 待转发消息的Index，默认值为-1
         */
        private long writeIndex = DLedgerEntryPusher.this.dLedgerStore.getLedgerEndIndex() + 1;

        /**
         * the index of the last entry to be pushed to this peer(initialized to -1)
         */
        private long matchIndex = -1;

        private final int maxPendingSize = 1000;
        private long term = -1;
        private String leaderId = null;
        private long lastCheckLeakTimeMs = System.currentTimeMillis();

        /**
         * KEY为消息的INDEX，value为该条消息向Follwer节点转发的时间
         */
        private final ConcurrentMap<Long/*index*/, Pair<Long/*send timestamp*/, Integer/*entries count in req*/>> pendingMap = new ConcurrentHashMap<>();
        private final PushEntryRequest batchAppendEntryRequest = new PushEntryRequest();

        private long lastAppendEntryRequestSendTimeMs = -1;

        private final Quota quota = new Quota(dLedgerConfig.getPeerPushQuota());

        public EntryDispatcher(String peerId, Logger logger) {
            super("EntryDispatcher-" + memberState.getSelfId() + "-" + peerId, logger);
            this.peerId = peerId;
        }

        @Override
        public synchronized void start() {
            super.start();
            // initialize write index
            writeIndex = DLedgerEntryPusher.this.dLedgerStore.getLedgerEndIndex() + 1;
        }

        /**
         * 如果Term与memberState记录的不一致或者LeaderId为空或者LeaderId与memberState的不一致，会调用changeState方法，将消息的推送类型更改为COMPARE
         * ，并将compareIndex置为-1
         * @return
         */
        private boolean checkNotLeaderAndFreshState() {
            // 如果不是Leader节点
            if (!memberState.isLeader()) {
                return true;
            }
            // 如果Term与memberState记录的不一致或者LeaderId为空或者LeaderId与memberState的不一致
            if (term != memberState.currTerm() || leaderId == null || !leaderId.equals(memberState.getLeaderId())) {
                // 加锁
                synchronized (memberState) {
                    if (!memberState.isLeader()) {
                        return true;
                    }
                    PreConditions.check(memberState.getSelfId().equals(memberState.getLeaderId()), DLedgerResponseCode.UNKNOWN);
                    logger.info("[Push-{}->{}]Update term: {} and leaderId: {} to new term: {}, new leaderId: {}", selfId, peerId, term, leaderId, memberState.currTerm(), memberState.getLeaderId());
                    term = memberState.currTerm();
                    leaderId = memberState.getSelfId();
                    // 更改状态为COMPARE
                    changeState(EntryDispatcherState.COMPARE);
                }
            }
            return false;
        }

        private PushEntryRequest buildCompareOrTruncatePushRequest(long preLogTerm, long preLogIndex,
            PushEntryRequest.Type type) {
            PushEntryRequest request = new PushEntryRequest();
            request.setGroup(memberState.getGroup());
            request.setRemoteId(peerId);
            request.setLeaderId(leaderId);
            request.setLocalId(memberState.getSelfId());
            request.setTerm(term);
            request.setPreLogIndex(preLogIndex);
            request.setPreLogTerm(preLogTerm);
            request.setType(type);
            request.setCommitIndex(memberState.getCommittedIndex());
            return request;
        }

        private PushEntryRequest buildCommitPushRequest() {
            PushEntryRequest request = new PushEntryRequest();
            request.setGroup(memberState.getGroup());
            request.setRemoteId(peerId);
            request.setLeaderId(leaderId);
            request.setLocalId(memberState.getSelfId());
            request.setTerm(term);
            request.setType(PushEntryRequest.Type.COMMIT);
            request.setCommitIndex(memberState.getCommittedIndex());
            return request;
        }

        private InstallSnapshotRequest buildInstallSnapshotRequest(DownloadSnapshot snapshot) {
            InstallSnapshotRequest request = new InstallSnapshotRequest();
            request.setGroup(memberState.getGroup());
            request.setRemoteId(peerId);
            request.setLeaderId(leaderId);
            request.setLocalId(memberState.getSelfId());
            request.setTerm(term);
            request.setLastIncludedIndex(snapshot.getMeta().getLastIncludedIndex());
            request.setLastIncludedTerm(snapshot.getMeta().getLastIncludedTerm());
            request.setData(snapshot.getData());
            return request;
        }

        private void resetBatchAppendEntryRequest() {
            batchAppendEntryRequest.setGroup(memberState.getGroup());
            batchAppendEntryRequest.setRemoteId(peerId);
            batchAppendEntryRequest.setLeaderId(leaderId);
            batchAppendEntryRequest.setLocalId(selfId);
            batchAppendEntryRequest.setTerm(term);
            batchAppendEntryRequest.setType(PushEntryRequest.Type.APPEND);
            batchAppendEntryRequest.clear();
        }

        private void checkQuotaAndWait(DLedgerEntry entry) {
            if (dLedgerStore.getLedgerEndIndex() - entry.getIndex() <= maxPendingSize) {
                return;
            }
            quota.sample(entry.getSize());
            if (quota.validateNow()) {
                long leftNow = quota.leftNow();
                logger.warn("[Push-{}]Quota exhaust, will sleep {}ms", peerId, leftNow);
                DLedgerUtils.sleep(leftNow);
            }
        }

        private DLedgerEntry getDLedgerEntryForAppend(long index) {
            DLedgerEntry entry;
            try {
                entry = dLedgerStore.get(index);
            } catch (DLedgerException e) {
                //  Do compare, in case the ledgerBeginIndex get refreshed.
                if (DLedgerResponseCode.INDEX_LESS_THAN_LOCAL_BEGIN.equals(e.getCode())) {
                    logger.info("[Push-{}]Get INDEX_LESS_THAN_LOCAL_BEGIN when requested index is {}, try to compare", peerId, index);
                    return null;
                }
                throw e;
            }
            PreConditions.check(entry != null, DLedgerResponseCode.UNKNOWN, "writeIndex=%d", index);
            return entry;
        }

        private void doCommit() throws Exception {
            if (DLedgerUtils.elapsed(lastPushCommitTimeMs) > 1000) {
                PushEntryRequest request = buildCommitPushRequest();
                //Ignore the results
                dLedgerRpcService.push(request);
                lastPushCommitTimeMs = System.currentTimeMillis();
            }
        }

        private void doCheckAppendResponse() throws Exception {
            long peerWaterMark = getPeerWaterMark(term, peerId);
            Pair<Long, Integer> pair = pendingMap.get(peerWaterMark + 1);
            if (pair == null)
                return;
            long sendTimeMs = pair.getKey();
            if (DLedgerUtils.elapsed(sendTimeMs) > dLedgerConfig.getMaxPushTimeOutMs()) {
                // reset write index
                batchAppendEntryRequest.clear();
                writeIndex = peerWaterMark + 1;
                logger.warn("[Push-{}]Reset write index to {} for resending the entries which are timeout", peerId, peerWaterMark + 1);
            }
        }

        private synchronized void changeState(EntryDispatcherState target) {
            logger.info("[Push-{}]Change state from {} to {}, matchIndex: {}, writeIndex: {}", peerId, type.get(), target, matchIndex, writeIndex);
            switch (target) {
                case APPEND:
                    resetBatchAppendEntryRequest();
                    break;
                case COMPARE:
                    // 如果设置COMPARE状态成功
                    if (this.type.compareAndSet(EntryDispatcherState.APPEND, EntryDispatcherState.COMPARE)) {
                        writeIndex = dLedgerStore.getLedgerEndIndex() + 1;
                        pendingMap.clear();
                    }
                    break;
                default:
                    break;
            }
            type.set(target);
        }

        /**
         * 1. 在doWork方法中，首先调用checkAndFreshState校验节点的状态，这一步主要是校验当前节点是否是Leader节点以及更改消息的推送类型，如果不是Leader节点结束处理，如果是Leader节点，对消息的推送类型进行判断：
         *      1. APPEND：消息追加，用于向Follower转发消息，批量消息调用doBatchAppend，否则调用doAppend处理；
         *      2. COMPARE：消息对比，一般出现在数据不一致的情况下，此时调用doCompare对比消息；
         *
         */
        @Override
        public void doWork() {
            try {
                // 检查状态
                if (checkNotLeaderAndFreshState()) {
                    // 不是Leader节点结束处理
                    waitForRunning(1);
                    return;
                }
                switch (type.get()) {
                    case COMPARE:// 对比
                        doCompare();
                        break;
                    case TRUNCATE:
                        doTruncate();
                        break;
                    case APPEND:// 如果是APPEND类型
                        doAppend();
                        break;
                    case INSTALL_SNAPSHOT:
                        doInstallSnapshot();
                        break;
                    case COMMIT:
                        doCommit();
                        break;
                }
                waitForRunning(1);
            } catch (Throwable t) {
                DLedgerEntryPusher.LOGGER.error("[Push-{}]Error in {} writeIndex={} matchIndex={}", peerId, getName(), writeIndex, matchIndex, t);
                // 出现异常转为COMPARE
                changeState(EntryDispatcherState.COMPARE);
                DLedgerUtils.sleep(500);
            }
        }

        /**
         * First compare the leader store with the follower store, find the match index for the follower, and update write index to [matchIndex + 1]
         * (首先比较leader存储和follower存储，找到follower的匹配索引，并将write索引更新为[matchIndex + 1])
         *
         * Leader节点消息比较
         * 1. 处于以下两种情况之一时，会认为数据出现了不一致的情况，将状态更改为Compare：
         *  （1）Leader节点在调用checkAndFreshState检查的时候，发现当前Term与memberState记录的不一致或者LeaderId为空或者LeaderId与memberState记录的LeaderId不一致；
         *  （2）Follower节点在处理消息APPEND请求在进行校验的时候（Follower节点请求校验链接），发现数据出现了不一致，会在请求的响应中设置不一致的状态INCONSISTENT_STATE，通知Leader节点；
         *
         * 2. 在COMPARE状态下，会调用doCompare方法向Follower节点发送比较请求，处理逻辑如下：
         *      1. 调用checkAndFreshState校验状态；
         *      2. 判断是否是COMPARE或者TRUNCATE请求，如果不是终止处理；
         *      3. 如果compareIndex为-1（changeState方法将状态改为COMPARE时中会将compareIndex置为-1），获取LedgerEndIndex作为compareIndex的值进行更新；
         *      4. 如果compareIndex的值大于LedgerEndIndex或者小于LedgerBeginIndex，依旧使用LedgerEndIndex作为compareIndex的值，所以单独加一个判断条件应该是为了打印日志，与第3步做区分；
         *      5. 根据compareIndex获取消息entry对象，调用buildPushRequest方法构建COMPARE请求；
         *      6. 向Follower节点推送建COMPARE请求进行比较，这里可以快速跳转到Follwer节点对COMPARE请求的处理；
         *   状态更改为COMPARE之后，compareIndex的值会被初始化为-1，在doCompare中，会将compareIndex的值更改为Leader节点的最后一条写入的消息
         *   ，也就是LedgerEndIndex的值，发给Follower节点进行对比。
         *
         * 3. 向Follower节点发起请求后，等待COMPARE请求返回响应，请求中会将Follower节点最后成功写入的消息的index设置在响应对象的EndIndex变量中，第一条写入的消息记录在BeginIndex变量中：
         *      1. 请求响应成功：
         *          1. 如果compareIndex与follower返回请求中的EndIndex相等，表示没有数据不一致的情况，将状态更改为APPEND；
         *          2. 其他情况，将truncateIndex的值置为compareIndex；
         *      2. 如果请求中返回的EndIndex小于当前节点的LedgerBeginIndex，或者BeginIndex大于LedgerEndIndex，也就是follower与leader的index不相交时
         *         ， 将truncateIndex设置为Leader的BeginIndex；
         *              根据代码中的注释来看，这种情况通常发生在Follower节点出现故障了很长一段时间，在此期间Leader节点删除了一些过期的消息；
         *      3. compareIndex比follower的BeginIndex小，将truncateIndex设置为Leader的BeginIndex；
         *              根据代码中的注释来看，这种情况请通常发生在磁盘出现故障的时候。
         *      4. 其他情况，将compareIndex的值减一，从上一条消息开始继续对比；
         *      5. 如果truncateIndex的值不为-1，调用doTruncate方法进行处理；
         *
         *
         *
         *
         *
         * @throws Exception
         */
        private void doCompare() throws Exception {
            while (true) {
                // 校验状态
                if (checkNotLeaderAndFreshState()) {
                    break;
                }
                // 如果不是COMPARE请求也不是TRUNCATE请求
                if (this.type.get() != EntryDispatcherState.COMPARE) {
                    break;
                }
                // 如果compareIndex为-1并且LedgerEndIndex为-1
                if (dLedgerStore.getLedgerEndIndex() == -1) {
                    // now not entry in store
                    break;
                }

                // compare process start from the [nextIndex -1]
                PushEntryRequest request;
                long compareIndex = writeIndex - 1;
                long compareTerm = -1;
                // 如果compareIndex为-1
                if (compareIndex < dLedgerStore.getLedgerBeforeBeginIndex()) {
                    // need compared entry has been dropped for compaction, just change state to install snapshot
                    changeState(EntryDispatcherState.INSTALL_SNAPSHOT);
                    return;
                } else if (compareIndex == dLedgerStore.getLedgerBeforeBeginIndex()) {
                    compareTerm = dLedgerStore.getLedgerBeforeBeginTerm();
                    request = buildCompareOrTruncatePushRequest(compareTerm, compareIndex, PushEntryRequest.Type.COMPARE);
                } else {
                    DLedgerEntry entry = dLedgerStore.get(compareIndex);
                    PreConditions.check(entry != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                    compareTerm = entry.getTerm();
                    request = buildCompareOrTruncatePushRequest(compareTerm, entry.getIndex(), PushEntryRequest.Type.COMPARE);
                }
                // 发送COMPARE请求
                CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request);
                // 获取响应结果
                PushEntryResponse response = responseFuture.get(3, TimeUnit.SECONDS);
                PreConditions.check(response != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                PreConditions.check(response.getCode() == DLedgerResponseCode.INCONSISTENT_STATE.getCode() || response.getCode() == DLedgerResponseCode.SUCCESS.getCode()
                    , DLedgerResponseCode.valueOf(response.getCode()), "compareIndex=%d", compareIndex);

                // fast backup algorithm to locate the match index
                // 如果返回成功
                if (response.getCode() == DLedgerResponseCode.SUCCESS.getCode()) {
                    // leader find the matched index for this follower
                    matchIndex = compareIndex;
                    // 更新水位
                    updatePeerWaterMark(compareTerm, peerId, matchIndex);
                    // change state to truncate

                    changeState(EntryDispatcherState.TRUNCATE);
                    return;
                }

                // inconsistent state, need to keep comparing
                if (response.getXTerm() != -1) {
                    writeIndex = response.getXIndex();
                } else {
                    writeIndex = response.getEndIndex() + 1;
                }
            }
        }

        /**
         * 在doTruncate方法中，会构建TRUNCATE请求设置truncateIndex(要删除的消息的index)，发送给Follower节点，通知Follower节点将数据不一致的那条消息删除
         * ，如果响应成功，可以看到接下来调用了changeState将状态改为APPEND，在changeState中，调用了updatePeerWaterMark更新节点的复制进度为出现数据不一致的那条消息的index
         * ，同时也更新了writeIndex，下次从writeIndex处重新给Follower节点发送APPEND请求进行消息写入：
         * @throws Exception
         */
        private void doTruncate() throws Exception {
            PreConditions.check(type.get() == EntryDispatcherState.TRUNCATE, DLedgerResponseCode.UNKNOWN);
            // truncate all entries after truncateIndex for follower
            long truncateIndex = matchIndex + 1;
            logger.info("[Push-{}]Will push data to truncate truncateIndex={}", peerId, truncateIndex);
            // 构建TRUNCATE请求
            PushEntryRequest truncateRequest = buildCompareOrTruncatePushRequest(-1, truncateIndex, PushEntryRequest.Type.TRUNCATE);
            // 向Folower节点发送TRUNCATE请求
            PushEntryResponse truncateResponse = dLedgerRpcService.push(truncateRequest).get(3, TimeUnit.SECONDS);

            PreConditions.check(truncateResponse != null, DLedgerResponseCode.UNKNOWN, "truncateIndex=%d", truncateIndex);
            PreConditions.check(truncateResponse.getCode() == DLedgerResponseCode.SUCCESS.getCode(), DLedgerResponseCode.valueOf(truncateResponse.getCode()), "truncateIndex=%d", truncateIndex);
            lastPushCommitTimeMs = System.currentTimeMillis();
            // 更改回APPEND状态
            changeState(EntryDispatcherState.APPEND);
        }

        /**
         * 1. 如果处于APPEND状态，Leader节点会向Follower节点发送Append请求，将消息转发给Follower节点，doAppend方法的处理逻辑如下：
         *      1. 调用checkAndFreshState进行状态检查；
         *      2. 判断推送类型是否是APPEND，如果不是终止处理；
         *      3. writeIndex为待转发消息的Index，默认值为-1，判断是否大于LedgerEndIndex，如果大于调用doCommit向Follower节点发送COMMIT请求更新committedIndex（后面再说）；
         *          这里可以看出转发日志的时候也使用了一个计数器writeIndex来记录待转发消息的index，每次根据writeIndex的值从日志中取出消息进行转发
         *          ，转发成后更新writeIndex的值（自增）指向下一条数据。
         *      4. 如果pendingMap中的大小超过了最大限制maxPendingSize的值，或者上次检查时间超过了1000ms（有较长的时间未进行清理），进行过期数据清理（这一步主要就是为了清理数据）：
         *          pendingMap是一个ConcurrentMap，KEY为消息的INDEX，value为该条消息向Follwer节点转发的时间（doAppendInner方法中会将数据加入到pendingMap）；
         *          1. 前面知道peerWaterMark的数据记录了每个节点的消息复制进度，这里根据Term和节点ID获取对应的复制进度（最新复制成功的消息的index）记在peerWaterMark变量中；
         *          2. 遍历pendingMap，与peerWaterMark的值对比，peerWaterMark之前的消息表示都已成功的写入完毕，所以小于peerWaterMark说明已过期可以被清理掉，将数据从pendingMap移除达到清理空间的目的；
         *          3. 更新检查时间lastCheckLeakTimeMs的值为当前时间；
         *      5. 调用doAppendInner方法转发消息；
         *      6. 更新writeIndex的值，做自增操作指向下一条待转发的消息index；
         *
         *
         * @throws Exception
         */
        private void doAppend() throws Exception {
            while (true) {
                // 校验状态
                if (checkNotLeaderAndFreshState()) {
                    break;
                }
                // 如果不是APPEND状态，终止
                if (type.get() != EntryDispatcherState.APPEND) {
                    break;
                }
                // check if first append request is timeout now
                doCheckAppendResponse();
                // check if now not new entries to be sent
                // 判断待转发消息的Index是否大于LedgerEndIndex
                if (writeIndex > dLedgerStore.getLedgerEndIndex()) {
                    if (this.batchAppendEntryRequest.getCount() > 0) {
                        // 发送批量追加的请求，并更新水位线
                        sendBatchAppendEntryRequest();
                    } else {
                        // 向Follower节点发送COMMIT请求更新
                        doCommit();
                    }
                    break;
                }
                // check if now not entries in store can be sent
                if (writeIndex <= dLedgerStore.getLedgerBeforeBeginIndex()) {
                    logger.info("[Push-{}]The ledgerBeginBeginIndex={} is less than or equal to  writeIndex={}", peerId, dLedgerStore.getLedgerBeforeBeginIndex(), writeIndex);
                    changeState(EntryDispatcherState.INSTALL_SNAPSHOT);
                    break;
                }
                // 如果pendingMap中的大小超过了maxPendingSize，或者上次检查时间超过了1000ms
                if (pendingMap.size() >= maxPendingSize || DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000) {
                    // 根据节点peerId获取复制进度
                    long peerWaterMark = getPeerWaterMark(term, peerId);
                    // 遍历pendingMap
                    for (Map.Entry<Long, Pair<Long, Integer>> entry : pendingMap.entrySet()) {
                        // 如果index小于peerWaterMark
                        if (entry.getKey() + entry.getValue().getValue() - 1 <= peerWaterMark) {
                            // clear the append request which all entries have been accepted in peer 清除所有条目都已被peer接受的追加请求
                            // 移除
                            pendingMap.remove(entry.getKey());
                        }
                    }
                    // 更新检查时间
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                if (pendingMap.size() >= maxPendingSize) {
                    doCheckAppendResponse();
                    break;
                }
                // 同步消息 并更新水位线
                long lastIndexToBeSend = doAppendInner(writeIndex);
                if (lastIndexToBeSend == -1) {
                    break;
                }
                // 更新writeIndex的值
                writeIndex = lastIndexToBeSend + 1;
            }
        }

        /**
         * append the entries to the follower, append it in memory until the threshold is reached, its will be really sent to peer
         * （将条目追加到follower，将其追加到内存中，直到达到阈值，才会真正发送给peer）
         * 从leader转发消息
         * 1. doAppendInner的处理逻辑如下：
         *      1. 根据消息的index从日志获取消息Entry；
         *      2. 调用buildPushRequest方法构建日志转发请求PushEntryRequest，在请求中设置了消息entry、当前Term
         *      、Leader节点的commitIndex（最后一条得到集群中大多数节点响应的消息index）等信息；
         *      3. 调用dLedgerRpcService的push方法将请求发送给Follower节点；
         *      4. 将本条消息对应的index加入到pendingMap中记录消息的发送时间（key为消息的index，value为当前时间）；
         *      5. 等待Follower节点返回响应：
         *          （1）如果响应状态为SUCCESS， 表示节点写入成功：
         *              从pendingMap中移除本条消息index的信息；
         *              更新当前节点的复制进度，也就是updatePeerWaterMark中的值；
         *              调用quorumAckChecker的wakeup，唤醒QuorumAckChecker线程；
         *          （2）如果响应状态为INCONSISTENT_STATE，表示Follower节点数据出现了不一致的情况，需要调用changeState更改状态为COMPARE；
         *
         * @param index from which index to append
         * @return the index of the last entry to be appended
         * @throws Exception
         */
        private long doAppendInner(long index) throws Exception {
            // 根据index从日志获取消息Entry
            DLedgerEntry entry = getDLedgerEntryForAppend(index);
            if (null == entry) {
                // means should install snapshot
                logger.error("[Push-{}]Get null entry from index={}", peerId, index);
                changeState(EntryDispatcherState.INSTALL_SNAPSHOT);
                return -1;
            }
            // check quota for flow controlling
            checkQuotaAndWait(entry);
            batchAppendEntryRequest.addEntry(entry);
            // check if now can trigger real send
            if (!dLedgerConfig.isEnableBatchAppend() || batchAppendEntryRequest.getTotalSize() >= dLedgerConfig.getMaxBatchAppendSize()
                || DLedgerUtils.elapsed(this.lastAppendEntryRequestSendTimeMs) >= dLedgerConfig.getMaxBatchAppendIntervalMs()) {
                sendBatchAppendEntryRequest();
            }
            return entry.getIndex();
        }

        private void sendBatchAppendEntryRequest() throws Exception {
            batchAppendEntryRequest.setCommitIndex(memberState.getCommittedIndex());
            final long firstIndex = batchAppendEntryRequest.getFirstEntryIndex();
            final long lastIndex = batchAppendEntryRequest.getLastEntryIndex();
            final long lastTerm = batchAppendEntryRequest.getLastEntryTerm();
            final long entriesCount = batchAppendEntryRequest.getCount();
            final long entriesSize = batchAppendEntryRequest.getTotalSize();
            StopWatch watch = StopWatch.createStarted();
            // 添加日志转发请求，发送给Follower节点
            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(batchAppendEntryRequest);
            // 加入到pendingMap中，key为消息的index，value为当前时间
            pendingMap.put(firstIndex, new Pair<>(System.currentTimeMillis(), batchAppendEntryRequest.getCount()));
            responseFuture.whenComplete((x, ex) -> {
                try {
                    // 处理请求响应
                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
                    switch (responseCode) {
                        case SUCCESS:// 如果成功
                            // 从pendingMap中移除
                            Attributes attributes = DLedgerMetricsManager.newAttributesBuilder().put(LABEL_REMOTE_ID, this.peerId).build();
                            DLedgerMetricsManager.replicateEntryLatency.record(watch.getTime(TimeUnit.MICROSECONDS), attributes);
                            DLedgerMetricsManager.replicateEntryBatchCount.record(entriesCount, attributes);
                            DLedgerMetricsManager.replicateEntryBatchBytes.record(entriesSize, attributes);
                            pendingMap.remove(firstIndex);
                            if (lastIndex > matchIndex) {
                                matchIndex = lastIndex;
                                // 更新水位线
                                updatePeerWaterMark(lastTerm, peerId, matchIndex);
                            }
                            break;
                        case INCONSISTENT_STATE:// 如果响应状态为INCONSISTENT_STATE
                            logger.info("[Push-{}]Get INCONSISTENT_STATE when append entries from {} to {} when term is {}", peerId, firstIndex, lastIndex, term);
                            // 转为COMPARE状态
                            changeState(EntryDispatcherState.COMPARE);
                            break;
                        default:
                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
                            break;
                    }
                } catch (Throwable t) {
                    logger.error("Failed to deal with the callback when append request return", t);
                }
            });
            lastPushCommitTimeMs = System.currentTimeMillis();
            batchAppendEntryRequest.clear();
        }

        private void doInstallSnapshot() throws Exception {
            // get snapshot from snapshot manager
            if (checkNotLeaderAndFreshState()) {
                return;
            }
            if (type.get() != EntryDispatcherState.INSTALL_SNAPSHOT) {
                return;
            }
            if (fsmCaller.getSnapshotManager() == null) {
                logger.error("[DoInstallSnapshot-{}]snapshot mode is disabled", peerId);
                changeState(EntryDispatcherState.COMPARE);
                return;
            }
            SnapshotManager manager = fsmCaller.getSnapshotManager();
            SnapshotReader snpReader = manager.getSnapshotReaderIncludedTargetIndex(writeIndex);
            if (snpReader == null) {
                logger.error("[DoInstallSnapshot-{}]get latest snapshot whose lastIncludedIndex >= {}  failed", peerId, writeIndex);
                changeState(EntryDispatcherState.COMPARE);
                return;
            }
            DownloadSnapshot snapshot = snpReader.generateDownloadSnapshot();
            if (snapshot == null) {
                logger.error("[DoInstallSnapshot-{}]generate latest snapshot for download failed, index = {}", peerId, writeIndex);
                changeState(EntryDispatcherState.COMPARE);
                return;
            }
            long lastIncludedIndex = snapshot.getMeta().getLastIncludedIndex();
            long lastIncludedTerm = snapshot.getMeta().getLastIncludedTerm();
            InstallSnapshotRequest request = buildInstallSnapshotRequest(snapshot);
            StopWatch watch = StopWatch.createStarted();
            CompletableFuture<InstallSnapshotResponse> future = DLedgerEntryPusher.this.dLedgerRpcService.installSnapshot(request);
            InstallSnapshotResponse response = future.get(3, TimeUnit.SECONDS);
            PreConditions.check(response != null, DLedgerResponseCode.INTERNAL_ERROR, "installSnapshot lastIncludedIndex=%d", writeIndex);
            DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(response.getCode());
            switch (responseCode) {
                case SUCCESS:
                    Attributes attributes = DLedgerMetricsManager.newAttributesBuilder().put(LABEL_REMOTE_ID, this.peerId).build();
                    DLedgerMetricsManager.installSnapshotLatency.record(watch.getTime(TimeUnit.MICROSECONDS), attributes);
                    logger.info("[DoInstallSnapshot-{}]install snapshot success, lastIncludedIndex = {}, lastIncludedTerm", peerId, lastIncludedIndex, lastIncludedTerm);
                    if (lastIncludedIndex > matchIndex) {
                        matchIndex = lastIncludedIndex;
                        writeIndex = matchIndex + 1;
                    }
                    changeState(EntryDispatcherState.APPEND);
                    break;
                case INSTALL_SNAPSHOT_ERROR:
                case INCONSISTENT_STATE:
                    logger.info("[DoInstallSnapshot-{}]install snapshot failed, index = {}, term = {}", peerId, writeIndex, term);
                    changeState(EntryDispatcherState.COMPARE);
                    break;
                default:
                    logger.warn("[DoInstallSnapshot-{}]install snapshot failed because error response: code = {}, mas = {}, index = {}, term = {}", peerId, responseCode, response.baseInfo(), writeIndex, term);
                    changeState(EntryDispatcherState.COMPARE);
                    break;
            }
        }

    }

    enum EntryDispatcherState {
        COMPARE,
        TRUNCATE,
        APPEND,
        INSTALL_SNAPSHOT,
        COMMIT
    }

    /**
     * This thread will be activated by the follower.
     * Accept the push request and order it by the index, then append to ledger store one by one.
     * 该线程将由追随者激活。接受推送请求并按索引排序，然后逐一追加到分类账存储中。
     *
     * 1. EntryHandler用于Follower节点处理Leader发送的消息请求，对请求的处理在handlePush方法中，根据请求类型的不同做如下处理：
     *      1. 如果是APPEND请求，将请求加入到writeRequestMap中；
     *      2. 如果是COMMIT请求，将请求加入到compareOrTruncateRequests；
     *      3. 如果是COMPARE或者TRUNCATE，将请求加入到compareOrTruncateRequests；
     * handlePush方法中，并没有直接处理请求，而是将不同类型的请求加入到不同的请求集合中，请求的处理是另外一个线程在doWork方法中处理的。
     *
     * 2. EntryHandler同样继承了ShutdownAbleThread，所以会启动线程执行doWork方法，在doWork方法中对请求进行了处理：
     *
     */
    private class EntryHandler extends ShutdownAbleThread {

        private long lastCheckFastForwardTimeMs = System.currentTimeMillis();

        ConcurrentMap<Long/*index*/, Pair<PushEntryRequest/*request*/, CompletableFuture<PushEntryResponse/*complete future*/>>> writeRequestMap = new ConcurrentHashMap<>();
        BlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>>
            compareOrTruncateRequests = new ArrayBlockingQueue<>(1024);

        private ReentrantLock inflightInstallSnapshotRequestLock = new ReentrantLock();

        private Pair<InstallSnapshotRequest, CompletableFuture<InstallSnapshotResponse>> inflightInstallSnapshotRequest;

        public EntryHandler(Logger logger) {
            super("EntryHandler-" + memberState.getSelfId(), logger);
        }

        public CompletableFuture<InstallSnapshotResponse> handleInstallSnapshot(InstallSnapshotRequest request) {
            CompletableFuture<InstallSnapshotResponse> future = new TimeoutFuture<>(1000);
            PreConditions.check(request.getData() != null && request.getData().length > 0, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
            long index = request.getLastIncludedIndex();
            inflightInstallSnapshotRequestLock.lock();
            try {
                CompletableFuture<InstallSnapshotResponse> oldFuture = null;
                if (inflightInstallSnapshotRequest != null && inflightInstallSnapshotRequest.getKey().getLastIncludedIndex() >= index) {
                    oldFuture = future;
                    logger.warn("[MONITOR]The install snapshot request with index {} has already existed", index, inflightInstallSnapshotRequest.getKey());
                } else {
                    logger.warn("[MONITOR]The install snapshot request with index {} preempt inflight slot because of newer index", index);
                    if (inflightInstallSnapshotRequest != null && inflightInstallSnapshotRequest.getValue() != null) {
                        oldFuture = inflightInstallSnapshotRequest.getValue();
                    }
                    inflightInstallSnapshotRequest = new Pair<>(request, future);
                }
                if (oldFuture != null) {
                    InstallSnapshotResponse response = new InstallSnapshotResponse();
                    response.setGroup(request.getGroup());
                    response.setCode(DLedgerResponseCode.NEWER_INSTALL_SNAPSHOT_REQUEST_EXIST.getCode());
                    response.setTerm(request.getTerm());
                    oldFuture.complete(response);
                }
            } finally {
                inflightInstallSnapshotRequestLock.unlock();
            }
            return future;
        }

        public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
            // The timeout should smaller than the remoting layer's request timeout
            CompletableFuture<PushEntryResponse> future = new TimeoutFuture<>(1000);
            switch (request.getType()) {
                case APPEND:  // 如果是Append
                    PreConditions.check(request.getCount() > 0, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                    long index = request.getFirstEntryIndex();
                    // 将请求加入到writeRequestMap
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> old = writeRequestMap.putIfAbsent(index, new Pair<>(request, future));
                    if (old != null) {
                        logger.warn("[MONITOR]The index {} has already existed with {} and curr is {}", index, old.getKey().baseInfo(), request.baseInfo());
                        future.complete(buildResponse(request, DLedgerResponseCode.REPEATED_PUSH.getCode()));
                    }
                    break;
                case COMMIT: // 如果是提交
                    synchronized (this) {
                        // 加入到compareOrTruncateRequests
                        if (!compareOrTruncateRequests.offer(new Pair<>(request, future))) {
                            logger.warn("compareOrTruncateRequests blockingQueue is full when put commit request");
                            future.complete(buildResponse(request, DLedgerResponseCode.PUSH_REQUEST_IS_FULL.getCode()));
                        }
                    }
                    break;
                case COMPARE:
                case TRUNCATE:
                    writeRequestMap.clear();
                    synchronized (this) {
                        // 加入到compareOrTruncateRequests
                        if (!compareOrTruncateRequests.offer(new Pair<>(request, future))) {
                            logger.warn("compareOrTruncateRequests blockingQueue is full when put compare or truncate request");
                            future.complete(buildResponse(request, DLedgerResponseCode.PUSH_REQUEST_IS_FULL.getCode()));
                        }
                    }
                    break;
                default:
                    logger.error("[BUG]Unknown type {} from {}", request.getType(), request.baseInfo());
                    future.complete(buildResponse(request, DLedgerResponseCode.UNEXPECTED_ARGUMENT.getCode()));
                    break;
            }
            wakeup();
            return future;
        }

        private PushEntryResponse buildResponse(PushEntryRequest request, int code) {
            // 构建请求响应
            PushEntryResponse response = new PushEntryResponse();
            response.setGroup(request.getGroup());
            // 设置响应状态
            response.setCode(code);
            // 设置Term
            response.setTerm(request.getTerm());
            // 如果不是COMMIT
            if (request.getType() != PushEntryRequest.Type.COMMIT) {
                // 设置Index
                response.setIndex(request.getFirstEntryIndex());
                response.setCount(request.getCount());
            }
            return response;
        }

        /**
         * 1. handleDoAppend方法用于处理Append请求，将Leader转发的消息写入到日志文件：
         *      1. 从请求中获取消息Entry，调用appendAsFollower方法将消息写入文件；
         *      2. 调用updateCommittedIndex方法将Leader请求中携带的commitIndex更新到Follower本地，后面在讲QuorumAckChecker时候会提到；
         *
         * @param writeIndex
         * @param request
         * @param future
         */
        private void handleDoAppend(long writeIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(writeIndex == request.getFirstEntryIndex(), DLedgerResponseCode.INCONSISTENT_STATE);
                for (DLedgerEntry entry : request.getEntries()) {
                    // 将消息写入日志
                    dLedgerStore.appendAsFollower(entry, request.getTerm(), request.getLeaderId());
                }
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                long committedIndex = Math.min(dLedgerStore.getLedgerEndIndex(), request.getCommitIndex());
                // 更新CommitIndex
                if (DLedgerEntryPusher.this.memberState.followerUpdateCommittedIndex(committedIndex)) {
                    DLedgerEntryPusher.this.fsmCaller.onCommitted(committedIndex);
                }
            } catch (Throwable t) {
                logger.error("[HandleDoAppend] writeIndex={}", writeIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
        }

        /**
         * 1. handleDoCompare用于处理COMPARE请求，compareIndex为需要比较的index，处理逻辑如下：
         *      1. 进行校验，主要判断compareIndex与请求中的Index是否一致，以及请求类型是否是COMPARE；
         *      2. 根据compareIndex获取消息Entry；
         *      3. 构建响应内容，在响应中设置当前节点以及同步的消息的BeginIndex和EndIndex；
         *
         *
         * @param request
         * @param future
         * @return
         */
        private CompletableFuture<PushEntryResponse> handleDoCompare(PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                // 校验请求类型是否是COMPARE
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMPARE, DLedgerResponseCode.UNKNOWN);
                // fast backup algorithm
                long preLogIndex = request.getPreLogIndex();
                long preLogTerm = request.getPreLogTerm();
                if (preLogTerm == -1 && preLogIndex == -1) {
                    // leader's entries is empty
                    future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                    return future;
                }
                if (dLedgerStore.getLedgerEndIndex() >= preLogIndex) {
                    long compareTerm = 0;
                    if (dLedgerStore.getLedgerBeforeBeginIndex() == preLogIndex) {
                        // the preLogIndex is smaller than the smallest index of the ledger, so just compare the snapshot last included term
                        compareTerm = dLedgerStore.getLedgerBeforeBeginTerm();
                    } else {
                        // there exist a log whose index is preLogIndex
                        // 获取Entry
                        DLedgerEntry local = dLedgerStore.get(preLogIndex);
                        compareTerm = local.getTerm();
                    }
                    if (compareTerm == preLogTerm) {
                        // the log's term is preLogTerm
                        // all matched!
                        future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                        return future;
                    }
                    // if the log's term is not preLogTerm, we need to find the first log of this term
                    DLedgerEntry firstEntryWithTargetTerm = dLedgerStore.getFirstLogOfTargetTerm(compareTerm, preLogIndex);
                    PreConditions.check(firstEntryWithTargetTerm != null, DLedgerResponseCode.INCONSISTENT_STATE);
                    PushEntryResponse response = buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode());
                    response.setXTerm(compareTerm);
                    response.setXIndex(firstEntryWithTargetTerm.getIndex());
                    // 构建请求响应，这里返回成功，说明数据没有出现不一致
                    future.complete(response);
                    return future;
                }
                // if there doesn't exist entry in preLogIndex, we return last entry index
                PushEntryResponse response = buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode());
                response.setEndIndex(dLedgerStore.getLedgerEndIndex());
                future.complete(response);
            } catch (Throwable t) {
                logger.error("[HandleDoCompare] preLogIndex={}, preLogTerm={}", request.getPreLogIndex(), request.getPreLogTerm(), t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        /**
         * Leader节点会向Follower节点发送COMMIT请求，COMMIT请求主要是更新Follower节点本地的committedIndex的值，记录集群中最新的那条获取大多数响应的消息的index，在后面QuorumAckChecker中还会看到
         *
         * @param committedIndex
         * @param request
         * @param future
         * @return
         */
        private CompletableFuture<PushEntryResponse> handleDoCommit(long committedIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(committedIndex == request.getCommitIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMMIT, DLedgerResponseCode.UNKNOWN);
                committedIndex = committedIndex <= dLedgerStore.getLedgerEndIndex() ? committedIndex : dLedgerStore.getLedgerEndIndex();
                // 更新committedIndex
                if (DLedgerEntryPusher.this.memberState.followerUpdateCommittedIndex(committedIndex)) {
                    DLedgerEntryPusher.this.fsmCaller.onCommitted(committedIndex);
                }
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                logger.error("[HandleDoCommit] committedIndex={}", request.getCommitIndex(), t);
                future.complete(buildResponse(request, DLedgerResponseCode.UNKNOWN.getCode()));
            }
            return future;
        }

        /**
         * Follower节点对Truncate的请求处理在handleDoTruncate方法中，主要是根据Leader节点发送的truncateIndex，进行数据删除，将truncateIndex之后的消息从日志中删除
         * @param truncateIndex truncateIndex为待删除的消息的index
         * @param request
         * @param future
         * @return
         */
        private CompletableFuture<PushEntryResponse> handleDoTruncate(long truncateIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                logger.info("[HandleDoTruncate] truncateIndex={}", truncateIndex);
                PreConditions.check(request.getType() == PushEntryRequest.Type.TRUNCATE, DLedgerResponseCode.UNKNOWN);
                // 进行删除
                long index = dLedgerStore.truncate(truncateIndex);
                PreConditions.check(index == truncateIndex - 1, DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                long committedIndex = request.getCommitIndex() <= dLedgerStore.getLedgerEndIndex() ? request.getCommitIndex() : dLedgerStore.getLedgerEndIndex();
                // 更新committedIndex
                if (DLedgerEntryPusher.this.memberState.followerUpdateCommittedIndex(committedIndex)) {
                    DLedgerEntryPusher.this.fsmCaller.onCommitted(committedIndex);
                }
            } catch (Throwable t) {
                logger.error("[HandleDoTruncate] truncateIndex={}", truncateIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        private void handleDoInstallSnapshot(InstallSnapshotRequest request,
            CompletableFuture<InstallSnapshotResponse> future) {
            InstallSnapshotResponse response = new InstallSnapshotResponse();
            response.setGroup(request.getGroup());
            response.copyBaseInfo(request);
            try {
                logger.info("[HandleDoInstallSnapshot] begin to install snapshot, request={}", request);
                DownloadSnapshot snapshot = new DownloadSnapshot(new SnapshotMeta(request.getLastIncludedIndex(), request.getLastIncludedTerm()), request.getData());
                if (!fsmCaller.getSnapshotManager().installSnapshot(snapshot)) {
                    response.code(DLedgerResponseCode.INSTALL_SNAPSHOT_ERROR.getCode());
                    future.complete(response);
                    return;
                }
                response.code(DLedgerResponseCode.SUCCESS.getCode());
                future.complete(response);
            } catch (Throwable t) {
                logger.error("[HandleDoInstallSnapshot] install snapshot failed, request={}", request, t);
                response.code(DLedgerResponseCode.INSTALL_SNAPSHOT_ERROR.getCode());
                future.complete(response);
            }
        }

        /**
         * 1. checkAppendFuture方法中的入参endIndex，表示当前待写入消息的index，也就是当前节点记录的最后一条成功写入的index（LedgerEndIndex）值加1，方法的处理逻辑如下：
         *      1. 将minFastForwardIndex初始化为最大值，minFastForwardIndex用于找到最小的那个出现数据不一致的消息index；
         *      2. 遍历writeRequestMap，处理每一个正在进行中的写入请求：
         *          （1）由于消息可能是批量的，所以获取当前请求中的第一条消息index，记为firstEntryIndex；
         *          （2）获取当前请求中的最后一条消息index，记为lastEntryIndex；
         *          （3）如果lastEntryIndex如果小于等于endIndex的值，进行如下处理：
         *              对比请求中的消息与当前节点存储的消息是否一致，如果是批量消息，遍历请求中的每一个消息，并根据消息的index从当前节的日志中获取消息进行对比，由于endIndex之前的消息都已成功写入，对应的写入请求还在writeRequestMap中表示可能由于某些原因未能从writeRequestMap中移除，所以如果数据对比一致的情况下可以将对应的请求响应设置为完成，并从writeRequestMap中移除；如果对比不一致，进入到异常处理，构建响应请求，状态设置为INCONSISTENT_STATE，通知Leader节点出现了数据不一致的情况；
         *          （4）如果第一条消息firstEntryIndex与endIndex + 1相等（这里不太明白为什么不是与endIndex 相等而是需要加1），表示该请求是endIndex之后的消息请求，结束本次检查；
         *          （5）判断当前请求的处理时间是否超时，如果未超时，继续处理下一个请求，如果超时进入到下一步；
         *          （6）走到这里，如果firstEntryIndex比minFastForwardIndex小，说明出现了数据不一致的情况，此时更新minFastForwardIndex，记录最小的那个数据不一致消息的index；
         *
         *      3. 如果minFastForwardIndex依旧是MAX_VALUE，表示没有数据不一致的消息，直接返回；
         *      4.根据minFastForwardIndex从writeRequestMap获取请求，如果获取为空，直接返回，否则调用buildBatchAppendResponse方法构建请求响应，表示数据出现了不一致，在响应中通知Leader节点；
         *
         *
         * @param endIndex
         */
        private void checkAppendFuture(long endIndex) {
            // 初始化为最大值
            long minFastForwardIndex = Long.MAX_VALUE;
            // 遍历writeRequestMap的value
            for (Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair : writeRequestMap.values()) {
                // 获取每个请求里面的第一条消息index
                long firstEntryIndex = pair.getKey().getFirstEntryIndex();
                // 获取每个请求里面的最后一条消息index
                long lastEntryIndex = pair.getKey().getLastEntryIndex();
                // clear old push request
                // 如果小于等于endIndex
                if (lastEntryIndex <= endIndex) {
                    try {
                        for (DLedgerEntry dLedgerEntry : pair.getKey().getEntries()) {
                            // 校验与当前节点存储的消息是否一致
                            PreConditions.check(dLedgerEntry.equals(dLedgerStore.get(dLedgerEntry.getIndex())), DLedgerResponseCode.INCONSISTENT_STATE);
                        }
                        // 设置完成
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.SUCCESS.getCode()));
                        logger.warn("[PushFallBehind]The leader pushed an batch append entry last index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", lastEntryIndex, endIndex);
                    } catch (Throwable t) {
                        logger.error("[PushFallBehind]The leader pushed an batch append entry last index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", lastEntryIndex, endIndex, t);
                        // 如果出现了异常，向Leader节点发送数据不一致的请求
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
                    }
                    // 处理之后从writeRequestMap移除
                    writeRequestMap.remove(pair.getKey().getFirstEntryIndex());
                    continue;
                }
                // normal case
                // 如果firstEntryIndex与endIndex + 1相等，表示该请求是endIndex之后的消息请求，结束本次检查
                if (firstEntryIndex == endIndex + 1) {
                    return;
                }
                // clear timeout push request
                // 判断响应是否超时，如果未超时，继续处理下一个
                TimeoutFuture<PushEntryResponse> future = (TimeoutFuture<PushEntryResponse>) pair.getValue();
                if (!future.isTimeOut()) {
                    continue;
                }
                // 如果firstEntryIndex比minFastForwardIndex小
                if (firstEntryIndex < minFastForwardIndex) {
                    // 更新minFastForwardIndex
                    minFastForwardIndex = firstEntryIndex;
                }
            }
            // 如果minFastForwardIndex依旧是MAX_VALUE，表示没有数据不一致的消息，直接返回
            if (minFastForwardIndex == Long.MAX_VALUE) {
                return;
            }
            // 根据minFastForwardIndex获取请求
            Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.remove(minFastForwardIndex);
            // 如果未获取到直接返回
            if (pair == null) {
                return;
            }
            logger.warn("[PushFastForward] ledgerEndIndex={} entryIndex={}", endIndex, minFastForwardIndex);
            // 向Leader返回响应，响应状态为INCONSISTENT_STATE
            pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
        }

        /**
         * The leader does push entries to follower, and record the pushed index. But in the following conditions, the push may get stopped.
         * * If the follower is abnormally shutdown, its ledger end index may be smaller than before. At this time, the leader may push fast-forward entries, and retry all the time.
         * * If the last ack is missed, and no new message is coming in.The leader may retry push the last message, but the follower will ignore it.
         *  Follower数据不一致检查
         * 1. checkAbnormalFuture方法用于检查数据的一致性，处理逻辑如下：
         *      1. 如果距离上次检查的时间未超过1000ms，直接返回；
         *      2. 更新检查时间lastCheckFastForwardTimeMs的值；
         *      3. 如果writeRequestMap为空表示目前没有写入请求，暂不需要处理；
         *      4. 调用checkAppendFuture方法进行检查；
         *
         *
         *
         * @param endIndex
         */
        private void checkAbnormalFuture(long endIndex) {
            // 如果距离上次检查的时间未超过1000ms
            if (DLedgerUtils.elapsed(lastCheckFastForwardTimeMs) < 1000) {
                return;
            }
            // 更新检查时间
            lastCheckFastForwardTimeMs = System.currentTimeMillis();
            // 如果writeRequestMap表示没有写入请求，暂不需要处理
            if (writeRequestMap.isEmpty()) {
                return;
            }

            // 检查
            checkAppendFuture(endIndex);
        }

        private void clearCompareOrTruncateRequestsIfNeed() {
            synchronized (this) {
                if (!memberState.isFollower() && !compareOrTruncateRequests.isEmpty()) {
                    List<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> drainList = new ArrayList<>();
                    compareOrTruncateRequests.drainTo(drainList);
                    for (Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair : drainList) {
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.NOT_FOLLOWER.getCode()));
                    }
                }
            }
        }


        /**
         * 1. EntryHandler同样继承了ShutdownAbleThread，所以会启动线程执行doWork方法，在doWork方法中对请求进行了处理：
         *      1. 如果compareOrTruncateRequests不为空，对请求类型进行判断：
         *          1. TRUNCATE：调用handleDoTruncate处理；
         *          2. COMPARE：调用handleDoCompare处理；
         *          3. COMMIT：调用handleDoCommit处理；
         *      2. 如果不是第1种情况，会认为是APPEND请求：
         *          （1）LedgerEndIndex记录了最后一条成功写入消息的index，对其 + 1表示下一条待写入消息的index；
         *          （2）根据待写入消息的index从writeRequestMap获取数据，如果获取为空，调用checkAbnormalFuture进行检查；
         *          （3）获取不为空，调用handleDoAppend方法处理消息写入；
         *          这里可以看出，Follower是从当前记录的最后一条成功写入的index（LedgerEndIndex），进行加1来处理下一条需要写入的消息的。
         *
         *
         *
         */
        @Override
        public void doWork() {
            try {
                // 判断是否是Follower
                if (!memberState.isFollower()) {
                    clearCompareOrTruncateRequestsIfNeed();
                    waitForRunning(1);

                    return;
                }
                // deal with install snapshot request first
                Pair<InstallSnapshotRequest, CompletableFuture<InstallSnapshotResponse>> installSnapshotPair = null;
                this.inflightInstallSnapshotRequestLock.lock();
                try {
                    if (inflightInstallSnapshotRequest != null && inflightInstallSnapshotRequest.getKey() != null && inflightInstallSnapshotRequest.getValue() != null) {
                        installSnapshotPair = inflightInstallSnapshotRequest;
                        inflightInstallSnapshotRequest = new Pair<>(null, null);
                    }
                } finally {
                    this.inflightInstallSnapshotRequestLock.unlock();
                }
                if (installSnapshotPair != null) {
                    handleDoInstallSnapshot(installSnapshotPair.getKey(), installSnapshotPair.getValue());
                }
                // deal with the compare or truncate requests
                // 如果compareOrTruncateRequests不为空
                if (compareOrTruncateRequests.peek() != null) {
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = compareOrTruncateRequests.poll();
                    PreConditions.check(pair != null, DLedgerResponseCode.UNKNOWN);
                    switch (pair.getKey().getType()) {
                        case TRUNCATE:
                            handleDoTruncate(pair.getKey().getPreLogIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMPARE:
                            handleDoCompare(pair.getKey(), pair.getValue());
                            break;
                        case COMMIT:
                            handleDoCommit(pair.getKey().getCommitIndex(), pair.getKey(), pair.getValue());
                            break;
                        default:
                            break;
                    }
                    return;
                }
                // 设置消息Index，为最后一条成功写入的消息index + 1
                long nextIndex = dLedgerStore.getLedgerEndIndex() + 1;
                // 从writeRequestMap取出请求
                Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.remove(nextIndex);
                // 如果获取的请求为空，调用checkAbnormalFuture进行检查
                if (pair == null) {
                    //checkAbnormalFuture 方法用于检查数据的一致性，
                    checkAbnormalFuture(dLedgerStore.getLedgerEndIndex());
                    waitForRunning(1);
                    return;
                }
                PushEntryRequest request = pair.getKey();

                // 处理
                handleDoAppend(nextIndex, request, pair.getValue());
            } catch (Throwable t) {
                DLedgerEntryPusher.LOGGER.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
        }
    }
}
