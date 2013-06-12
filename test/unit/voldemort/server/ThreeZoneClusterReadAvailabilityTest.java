/*
 * Copyright 2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.server;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ClusterTestUtils;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

/**
 * Provides a read availability test when nodes in all but one zone are down
 * 
 */
@RunWith(Parameterized.class)
public class ThreeZoneClusterReadAvailabilityTest {

    private String storeName = "";
    private final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                        10000,
                                                                                        100000,
                                                                                        32 * 1024);
    private final Cluster cluster;
    private final List<StoreDefinition> storeDefs;

    private StoreClient<String, String> storeClient;
    private Map<Integer, VoldemortServer> servers = new HashMap<Integer, VoldemortServer>();
    private final Integer clientZoneId;
    private final Integer availableZoneId;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { 0, 0 }, { 0, 1 }, { 0, 2 }, { 1, 0 }, { 1, 1 },
                { 1, 2 }, { 2, 0 }, { 2, 1 }, { 2, 2 } });
    }

    public ThreeZoneClusterReadAvailabilityTest(int clientZoneId, int availableZoneId) {
        this.cluster = ClusterTestUtils.getZZZCluster();
        this.storeDefs = ClusterTestUtils.getZZZ322StoreDefs("memory");
        this.storeName = storeDefs.get(0).getName();
        this.clientZoneId = clientZoneId;
        this.availableZoneId = availableZoneId;
    }

    @Before
    public void setUp() throws IOException {
        for(Integer nodeId: cluster.getNodeIds()) {
            VoldemortConfig config = ServerTestUtils.createServerConfigWithDefs(true,
                                                                                nodeId,
                                                                                TestUtils.createTempDir()
                                                                                         .getAbsolutePath(),
                                                                                cluster,
                                                                                storeDefs,
                                                                                new Properties());
            VoldemortServer server = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                          config);
            servers.put(nodeId, server);
        }

        Node node = cluster.getNodeById(0);
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        StoreClientFactory storeClientFactory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl)
                                                                                               .setClientZoneId(clientZoneId)
                                                                                               .enableDefaultClient(false));
        storeClient = storeClientFactory.getStoreClient(storeName);
        storeClient.put("Russia", "Moscow");
    }

    @After
    public void tearDown() {
        socketStoreFactory.close();
        for(VoldemortServer server: servers.values()) {
            server.stop();
        }
    }

    /**
     * Test get functionality when only zone 0 is available.
     */
    @Test
    public void testZone0Availability() {
        shutdownAllExceptZoneX(this.availableZoneId);
        try {
            storeClient.get("Russia");
        } catch(InsufficientOperationalNodesException e) {
            fail("Value unavailable though should be");
        }
    }

    private void shutdownAllExceptZoneX(int survivingZoneId) {
        for(VoldemortServer server: servers.values()) {
            Integer nodeId = server.getVoldemortConfig().getNodeId();
            Node node = cluster.getNodeById(nodeId);
            if(node.getZoneId() != survivingZoneId) {
                server.stop();
            }
        }
    }
}
