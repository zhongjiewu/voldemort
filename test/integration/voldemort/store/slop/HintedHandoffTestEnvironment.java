package voldemort.store.slop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.client.ClientConfig;
import voldemort.client.RoutingTier;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.common.service.ServiceType;
import voldemort.common.service.VoldemortService;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.server.storage.StorageService;
import voldemort.store.ForceFailStore;
import voldemort.store.PersistenceFailureException;
import voldemort.store.SleepyStore;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.memory.InMemoryPutAssertionStorageEngine;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;

public class HintedHandoffTestEnvironment implements Runnable {

    private final Logger logger = Logger.getLogger(HintedHandoffTestEnvironment.class);
    // basic configurations
    private final static String STORE_NAME = "test-store";
    private final static SerializerDefinition SEL_DEF = new SerializerDefinition("identity");
    private final static Integer NUM_NODES_TOTAL = 8;
    private final static Integer DEFAULT_REPLICATION_FACTOR = 2;
    private final static Integer DEFAULT_P_READS = 1;
    private final static Integer DEFAULT_R_READS = 1;
    private final static Integer DEFAULT_P_WRITES = 1;
    private final static Integer DEFAULT_R_WRITES = 1;
    private final static HintedHandoffStrategyType DEFAULT_HINT_ROUTING_STRATEGY = HintedHandoffStrategyType.PROXIMITY_STRATEGY;
    private int minNodesAvailable = 1;

    // cluster and servers
    private Cluster cluster = null;
    private final Map<Integer, VoldemortServer> voldemortServers = new HashMap<Integer, VoldemortServer>();
    private final CountDownLatch startFinishLatch = new CountDownLatch(1);
    private final CountDownLatch wrapUpRequestLatch = new CountDownLatch(1);
    private final CountDownLatch wrapUpFinishLatch = new CountDownLatch(1);

    // basic store
    private StoreDefinitionBuilder storeDefBuilder = new StoreDefinitionBuilder();
    private StoreDefinition storeDef = null;

    // stores
    private final Map<Integer, Store<ByteArray, byte[], byte[]>> realStores = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
    private final Map<Integer, ForceFailStore<ByteArray, byte[], byte[]>> forceFailStores = new HashMap<Integer, ForceFailStore<ByteArray, byte[], byte[]>>();
    private final Map<Integer, SleepyStore<ByteArray, byte[], byte[]>> sleepyStores = new HashMap<Integer, SleepyStore<ByteArray, byte[], byte[]>>();
    private final Map<Integer, SlopStorageEngine> slopStorageEngines = new HashMap<Integer, SlopStorageEngine>();

    // slop push
    private static Integer DEFAULT_SLOP_PUSH_INTERVAL_S = 10;

    // failures
    private final static Integer DEFAULT_REFRESH_INTERVAL_S = 8;
    private final static Integer DEFAULT_ASYNC_RECOVERY_INTERVAL_S = 5;
    private Integer statusRefreshIntervalSecond = DEFAULT_REFRESH_INTERVAL_S;
    private Map<Integer, NodeStatus> nodesStatus = new HashMap<Integer, NodeStatus>();

    // running thread
    private final Thread thread;

    // client and routing
    private StoreClientFactory factory;
    private RoutingStrategy routingStrategy = null;

    public static enum NodeStatus {
        NORMAL,
        DOWN,
        SLOW,
        BDB_ERROR
    }

    /**
     * A test environment used for hinted handoff test This environment
     * simulates multiple failures every several seconds The failure mode are
     * among BDB Exception, node down and slow response
     */
    public HintedHandoffTestEnvironment() {
        storeDefBuilder.setName(STORE_NAME)
                       .setType(InMemoryStorageConfiguration.TYPE_NAME)
                       .setKeySerializer(SEL_DEF)
                       .setValueSerializer(SEL_DEF)
                       .setRoutingPolicy(RoutingTier.CLIENT)
                       .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                       .setReplicationFactor(DEFAULT_REPLICATION_FACTOR)
                       .setPreferredReads(DEFAULT_P_READS)
                       .setRequiredReads(DEFAULT_R_READS)
                       .setPreferredWrites(DEFAULT_P_WRITES)
                       .setRequiredWrites(DEFAULT_R_WRITES)
                       .setHintedHandoffStrategy(DEFAULT_HINT_ROUTING_STRATEGY);
        thread = new Thread(this);
    }

    public HintedHandoffTestEnvironment setPreferredWrite(int number) {
        storeDefBuilder.setPreferredWrites(number);
        return this;
    }

    public HintedHandoffTestEnvironment setRequiredWrite(int number) {
        storeDefBuilder.setRequiredWrites(number);
        return this;
    }

    public HintedHandoffTestEnvironment setReplicationFactor(int number) {
        storeDefBuilder.setReplicationFactor(number);
        return this;
    }

    public void startServer(int nodeId) throws IOException {
        if(logger.isInfoEnabled())
            logger.info("Starting server of node [" + nodeId + "]");
        SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                              10000,
                                                                              100000,
                                                                              1024);
        List<StoreDefinition> stores = new ArrayList<StoreDefinition>();
        stores.add(storeDef);
        // start a voldemort server
        VoldemortConfig config = ServerTestUtils.createServerConfigWithDefs(true,
                                                                            nodeId,
                                                                            TestUtils.createTempDir()
                                                                                     .getAbsolutePath(),
                                                                            cluster,
                                                                            stores,
                                                                            new Properties());
        config.setNioAdminConnectorSelectors(1);
        config.setNioConnectorSelectors(5);
        config.setSlopFrequencyMs(DEFAULT_SLOP_PUSH_INTERVAL_S * 1000);
        config.setSlopStoreType("memory");
        config.setFailureDetectorAsyncRecoveryInterval(DEFAULT_ASYNC_RECOVERY_INTERVAL_S * 1000);

        VoldemortServer vs = ServerTestUtils.startVoldemortServer(socketStoreFactory, config);
        socketStoreFactory.close();
        voldemortServers.put(nodeId, vs);

        VoldemortService vsrv = vs.getService(ServiceType.STORAGE);
        StoreRepository sr = ((StorageService) vsrv).getStoreRepository();

        // storage engine injection
        sr.removeLocalStore(STORE_NAME);
        sr.addLocalStore(sleepyStores.get(nodeId));
        sr.removeStorageEngine(STORE_NAME);
        sr.addStorageEngine((StorageEngine<ByteArray, byte[], byte[]>) realStores.get(nodeId));

        // slop stores caching and injection
        if(!slopStorageEngines.containsKey(nodeId)) {
            SlopStorageEngine slopStorageEngine = sr.getSlopStore();
            slopStorageEngines.put(nodeId, slopStorageEngine);
        } else {
            sr.removeStorageEngine("slop");
            sr.removeLocalStore("slop");
            sr.addStorageEngine(slopStorageEngines.get(nodeId));
            sr.addLocalStore(slopStorageEngines.get(nodeId));
            sr.setSlopStore(slopStorageEngines.get(nodeId));
        }
    }

    public void stopServer(int nodeId) {
        if(logger.isInfoEnabled())
            logger.info("Stopping server of node [" + nodeId + "]");
        VoldemortServer server = voldemortServers.get(nodeId);
        server.stop();
    }

    public void createInnerStore(int nodeId) {
        Store<ByteArray, byte[], byte[]> realStore = new InMemoryPutAssertionStorageEngine<ByteArray, byte[], byte[]>(STORE_NAME);
        ForceFailStore<ByteArray, byte[], byte[]> forceFailStore = new ForceFailStore<ByteArray, byte[], byte[]>(realStore,
                                                                                                                 new PersistenceFailureException("Force failed"));
        SleepyStore<ByteArray, byte[], byte[]> sleepyStore = new SleepyStore<ByteArray, byte[], byte[]>(0,
                                                                                                        forceFailStore);
        realStores.put(nodeId, realStore);
        forceFailStores.put(nodeId, forceFailStore);
        sleepyStores.put(nodeId, sleepyStore);
    }

    @Override
    public void run() {
        Random random = new Random(System.currentTimeMillis());
        cluster = VoldemortTestConstants.getEightNodeClusterWithZones();
        storeDef = storeDefBuilder.build();
        // setup store engines
        for(Integer nodeId = 0; nodeId < NUM_NODES_TOTAL; nodeId++) {
            createInnerStore(nodeId); // do only once
        }

        for(Integer nodeId = 0; nodeId < NUM_NODES_TOTAL; nodeId++) {
            try {
                startServer(nodeId);
            } catch(IOException e) {
                logger.error("Server " + nodeId + "failed to start", e);
            }
        }

        // setup client factory
        String bootstrapUrl = cluster.getNodeById(0).getSocketUrl().toString();
        factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));

        // wait for start of servers
        startFinishLatch.countDown();

        try {
            boolean wrapUpSignal = false;
            while(!wrapUpSignal) {
                if(logger.isInfoEnabled()) {
                    logger.info("Will sleep for a while or until seeing wrapUpSignal. sleep time: "
                                + statusRefreshIntervalSecond + " Seconds");
                }
                wrapUpSignal = wrapUpRequestLatch.await(statusRefreshIntervalSecond,
                                                        TimeUnit.SECONDS);

                if(logger.isInfoEnabled()) {
                    if(wrapUpSignal) {
                        logger.info("Wake Up and wrap up. Make all servers NORMAL");
                        minNodesAvailable = NUM_NODES_TOTAL;
                    } else {
                        logger.info("Wake Up and decide new failure statuses");
                    }
                    for(Map.Entry<Integer, Store<ByteArray, byte[], byte[]>> entry: realStores.entrySet()) {
                        InMemoryPutAssertionStorageEngine<ByteArray, byte[], byte[]> engine = (InMemoryPutAssertionStorageEngine<ByteArray, byte[], byte[]>) entry.getValue();
                        logger.info("Outstanding Put Assertions of node [" + entry.getKey() + "]: "
                                    + engine.getFailedAssertions().size());
                    }
                }
                // decide random number of cluster nodes(at least 1 alive) with
                // random ids to fail
                Integer numNodesToFail = random.nextInt(NUM_NODES_TOTAL - minNodesAvailable + 1);
                Set<Integer> nodesToFail = getUniqueRandomNumbers(NUM_NODES_TOTAL, numNodesToFail);
                if(logger.isInfoEnabled()) {
                    logger.info("Setting nodes to Fail: " + nodesToFail.toString());
                }

                for(Integer nodeId = 0; nodeId < NUM_NODES_TOTAL; nodeId++) {
                    if(nodesToFail.contains(nodeId)) {
                        // fail a node if it's normal
                        if(nodesStatus.get(nodeId) == NodeStatus.NORMAL) {
                            // random pick one failure node
                            Integer failureMode = random.nextInt(3);
                            switch(failureMode) {
                                case 0:
                                    makeNodeDown(nodeId);
                                    break;
                                case 1:
                                    makeNodeSlow(nodeId);
                                    break;
                                case 2:
                                    makeNodeBdbError(nodeId);
                                    break;
                            }
                        }
                        // otherwise, leave unchanged
                    } else {
                        // make node normal if not normal
                        if(nodesStatus.get(nodeId) != NodeStatus.NORMAL) {
                            makeNodeNormal(nodeId);
                        }
                        // otherwise, leave unchanged
                    }
                }
            }
        } catch(InterruptedException e) {} finally {
            wrapUpFinishLatch.countDown();
        }
    }

    public Set<Integer> getUniqueRandomNumbers(int max, int count) {
        Set<Integer> result = new HashSet<Integer>();
        Random r = new Random();
        while(result.size() <= max && result.size() < count) {
            result.add(r.nextInt(max));
        }
        return result;
    }

    public void makeNodeDown(int nodeId) {
        if(nodesStatus.get(nodeId) != NodeStatus.DOWN) {
            if(logger.isInfoEnabled()) {
                logger.info("Setting Node[" + nodeId + "] to status [DOWN]");
            }
            makeNodeNormal(nodeId);
            stopServer(nodeId);
            nodesStatus.put(nodeId, NodeStatus.DOWN);
        }
    }

    public void makeNodeSlow(int nodeId) {
        if(nodesStatus.get(nodeId) != NodeStatus.SLOW) {
            if(logger.isInfoEnabled()) {
                logger.info("Setting Node[" + nodeId + "] to status [SLOW]");
            }
            makeNodeNormal(nodeId);
            sleepyStores.get(nodeId).setSleepTimeMs(100000);
            nodesStatus.put(nodeId, NodeStatus.SLOW);
        }
    }

    public void makeNodeBdbError(int nodeId) {
        if(nodesStatus.get(nodeId) != NodeStatus.BDB_ERROR) {
            if(logger.isInfoEnabled()) {
                logger.info("Setting Node[" + nodeId + "] to status [BDB_ERROR]");
            }
            makeNodeNormal(nodeId);
            forceFailStores.get(nodeId).setFail(true);
            nodesStatus.put(nodeId, NodeStatus.BDB_ERROR);
        }
    }

    public void makeNodeNormal(int nodeId) {
        NodeStatus status = nodesStatus.get(nodeId);
        if(status == null) {
            nodesStatus.put(nodeId, NodeStatus.NORMAL);
            status = NodeStatus.NORMAL;
        }

        if(status != NodeStatus.NORMAL) {
            if(logger.isInfoEnabled()) {
                logger.info("Setting Node[" + nodeId + "] to status [NORMAL]");
            }
        }

        if(status == NodeStatus.DOWN) {
            try {
                startServer(nodeId);
            } catch(IOException e) {
                logger.error("Server " + nodeId + "failed to start", e);
            }
        } else if(status == NodeStatus.SLOW) {
            sleepyStores.get(nodeId).setSleepTimeMs(0);
        } else if(status == NodeStatus.BDB_ERROR) {
            forceFailStores.get(nodeId).setFail(false);
        }
        nodesStatus.put(nodeId, NodeStatus.NORMAL);
    }

    public Store<ByteArray, byte[], byte[]> getRealStore(int nodeId) {
        return realStores.get(nodeId);
    }

    public List<Node> routeRequest(byte[] key) {
        if(routingStrategy == null) {
            routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef, cluster);
        }
        return routingStrategy.routeRequest(key);
    }

    public StoreClient<byte[], byte[]> makeClient() {
        return factory.getStoreClient(STORE_NAME);
    }

    public void waitForWrapUp() throws InterruptedException {
        if(logger.isInfoEnabled()) {
            logger.info("Waiting for wrap up");
        }
        // signal make all servers up
        wrapUpRequestLatch.countDown();
        // wait for all servers to come up
        wrapUpFinishLatch.await();
        if(logger.isInfoEnabled()) {
            logger.info("Finished waiting for wrap up");
            logger.info("Wait for slopPusherJob");
        }

        // wait until all slops are empty
        List<SlopStorageEngine> nonEmptySlopStorageEngines = new ArrayList<SlopStorageEngine>();
        nonEmptySlopStorageEngines.addAll(slopStorageEngines.values());
        while(nonEmptySlopStorageEngines.size() != 0) {
            SlopStorageEngine slopEngine = nonEmptySlopStorageEngines.get(0);
            ClosableIterator<ByteArray> it = slopEngine.keys();
            if(it.hasNext()) {
                Thread.sleep(100);
            } else {
                nonEmptySlopStorageEngines.remove(0);
                if(logger.isDebugEnabled()) {
                    logger.debug("One slop has been emptied. Waiting for "
                                 + nonEmptySlopStorageEngines.size() + " slopStores");
                }
            }
        }

        if(logger.isInfoEnabled()) {
            logger.info("Finished waiting for slopPusherJob");
        }
    }

    public void start() throws InterruptedException {
        if(logger.isInfoEnabled()) {
            logger.info("Starting up and wait");
        }
        thread.start();
        startFinishLatch.await();
        if(logger.isInfoEnabled()) {
            logger.info("Finished Waiting for start up");
        }
    }

    public void stop() {
        factory.close();
        for(Integer nodeId: voldemortServers.keySet()) {
            stopServer(nodeId);
        }
    }
}
