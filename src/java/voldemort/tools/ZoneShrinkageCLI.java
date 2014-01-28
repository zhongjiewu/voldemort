/**
 * Copyright 2014 LinkedIn, Inc
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
package voldemort.tools;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.log4j.Logger;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import static voldemort.VoldemortAdminTool.executeSetMetadataPair;
import static voldemort.store.metadata.MetadataStore.STORES_KEY;
import static voldemort.store.metadata.MetadataStore.CLUSTER_KEY;


/**
 * This tool change the cluster topology by dropping one zone
 */
public class ZoneShrinkageCLI {
    public static Logger logger = Logger.getLogger(ZoneShrinkageCLI.class);
    protected AdminClient adminClient;
    protected final Integer droppingZoneId;

    public static OptionParser getParser() {
        OptionParser parser = new OptionParser();
        parser.acceptsAll(Arrays.asList("url"), "Bootstrap URL of target cluster")
                .withRequiredArg()
                .ofType(String.class)
                .describedAs("bootstrap-url");
        parser.acceptsAll(Arrays.asList("drop-zoneid"), "ID of the zone to be dropped")
                .withRequiredArg()
                .ofType(Integer.class)
                .describedAs("zone-id");
        parser.acceptsAll(Arrays.asList("real-run"), "If this option is specified, the program will execute the shrinkage(Real Run). Otherwise, it will not actually execute the shrinkage");

        return parser;
    }

    public static void validateOptions(OptionSet options) throws IOException {
        Integer exitStatus = null;
        if(options.has("help")) {
            exitStatus = 0;
            System.out.println("This changes the targeted cluster topology by shrinking one zone.");
        }
        else if(!options.has("url")) {
            System.err.println("Option \"url\" is required");
            exitStatus = 1;
        }
        else if(!options.has("drop-zoneid")) {
            System.err.println("Option \"drop-zoneid\" is required");
            exitStatus = 1;
        }
        if(exitStatus != null) {
            if(exitStatus == 0)
                getParser().printHelpOn(System.out);
            else
                getParser().printHelpOn(System.err);
            System.exit(exitStatus);
        }
    }

    public static void main(String[] argv) throws Exception {
        OptionParser parser = getParser();
        OptionSet options = parser.parse(argv);
        validateOptions(options);

        ZoneShrinkageCLI cli = new ZoneShrinkageCLI((String)options.valueOf("url"), (Integer) options.valueOf("drop-zoneid"));
        cli.executeShrink(options.has("real-run"));
    }

    public ZoneShrinkageCLI(String url, Integer droppingZoneId) {
        AdminClientConfig acc = new AdminClientConfig();
        ClientConfig cc = new ClientConfig();
        adminClient = new AdminClient(url, acc, cc);
        this.droppingZoneId = droppingZoneId;
    }

    protected boolean verifyMetadata(Integer nodeId, String clusterXml, String storesXml) {
        boolean resultGood = true;
        Versioned<String> currentClusterXmlVersioned = adminClient.metadataMgmtOps.getRemoteMetadata(nodeId, CLUSTER_KEY);
        Versioned<String> currentStoresXmlVersioned = adminClient.metadataMgmtOps.getRemoteMetadata(nodeId, STORES_KEY);

        if(currentClusterXmlVersioned == null) {
            logger.error("Cluster XML does not exist on node " + nodeId);
            resultGood = false;
        } else {
            if(clusterXml.equals(currentClusterXmlVersioned.getValue())) {
                logger.info("Node " + nodeId + " cluster.xml is GOOD");
            } else {
                logger.info("Node " + nodeId + " cluster.xml is BAD");
                resultGood = false;
            }
        }
        if(currentStoresXmlVersioned == null) {
            logger.error("Stores XML does not exist on node " + nodeId);
            resultGood = false;
        } else {
            if(storesXml.equals(currentStoresXmlVersioned.getValue())) {
                logger.info("Node " + nodeId + " stores.xml is GOOD");
            } else {
                logger.info("Node " + nodeId + " stores.xml is BAD");
                resultGood = false;
            }
        }
        return resultGood;
    }

    public void executeShrink(boolean realRun) {
        String clusterXml;
        String storesXml;
        boolean shouldAbort = false;
        Cluster cluster = adminClient.getAdminClientCluster();
        Integer firstNodeId = cluster.getNodes().iterator().next().getId();


        // Get Metadata from one server
        logger.info("[Pre-Shrinkage] Start fetching metadata for server " + firstNodeId);
        clusterXml = adminClient.metadataMgmtOps.getRemoteMetadata(firstNodeId, CLUSTER_KEY).getValue();
        storesXml = adminClient.metadataMgmtOps.getRemoteMetadata(firstNodeId, STORES_KEY).getValue();

        logger.info("[Pre-Shrinkage] End fetching metadata for server " + firstNodeId);


        // Query the servers to see if all have the same XML
        logger.info("[Pre-Shrinkage] Checking metadata consistency on all servers");
        for(Node node: cluster.getNodes()) {
            boolean result = verifyMetadata(node.getId(), clusterXml, storesXml);
            shouldAbort = shouldAbort || !result;
        }
        logger.info("[Pre-Shrinkage] Finished checking metadata consistency on all servers");
        if(shouldAbort) {
            logger.info("Aborting");
            return;
        }

        // Calculate and print out the new metadata

        List<StoreDefinition> initialStoreDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(storesXml));
        List<StoreDefinition> finalStoreDefs = RebalanceUtils.dropZone(initialStoreDefs, droppingZoneId);

        Cluster initialCluster = new ClusterMapper().readCluster(new StringReader(clusterXml));
        Cluster intermediateCluster = RebalanceUtils.vacateZone(initialCluster, droppingZoneId);
        Cluster finalCluster = RebalanceUtils.dropZone(intermediateCluster, droppingZoneId);

        String newStoresXml = new StoreDefinitionsMapper().writeStoreList(finalStoreDefs);
        String newClusterXml =  new ClusterMapper().writeCluster(finalCluster);

        logger.info("New cluster.xml: \n" + newClusterXml);
        logger.info("New stores.xml: \n" + newStoresXml);

        // Verifying Server rebalancing states
        logger.info("[Pre-Shrinkage] Checking server states on all nodes");
        for(Node node: cluster.getNodes()) {
            Integer nodeId = node.getId();

            logger.info("[Pre-Shrinkage] Checking Node " + nodeId);
            Versioned<String> stateVersioned = adminClient.metadataMgmtOps.getRemoteMetadata(nodeId, MetadataStore.SERVER_STATE_KEY);
            if(stateVersioned == null) {
                logger.error("State object is null");
                shouldAbort = true;
            } else {
                if(!stateVersioned.getValue().equals("NORMAL_SERVER")) {
                    logger.error("Server state is not normal");
                    shouldAbort = true;
                }
            }
        }
        logger.info("[Pre-Shrinkage] Finished checking server states on all nodes");
        if(shouldAbort) {
            logger.info("Aborting");
            return;
        }

        // Run shrinkage
        logger.info("[Pre-Shrinkage] Updating metadata(cluster.xml, stores.xml) on all nodes");

        if(realRun) {
            executeSetMetadataPair(-1,
                    adminClient,
                    MetadataStore.CLUSTER_KEY,
                    newClusterXml,
                    MetadataStore.STORES_KEY,
                    newStoresXml);
        } else {
            logger.info("Skipping updating metadata (dry-run)");
        }

        // Check metadata consistency
        logger.info("[Post-Shrinkage] Start fetching metadata for server " + firstNodeId);

        clusterXml = adminClient.metadataMgmtOps.getRemoteMetadata(firstNodeId, CLUSTER_KEY).getValue();
        storesXml = adminClient.metadataMgmtOps.getRemoteMetadata(firstNodeId, STORES_KEY).getValue();

        logger.info("[Post-Shrinkage] End fetching metadata for server " + firstNodeId);

        logger.info("[Post-Shrinkage] Checking metadata consistency on all servers");
        for(Node node: cluster.getNodes()) {
            boolean result = verifyMetadata(node.getId(), clusterXml, storesXml);
            shouldAbort = shouldAbort || !result;
        }
        logger.info("[Post-Shrinkage] Finished checking metadata consistency on all servers");
        if(shouldAbort) {
            logger.info("Aborting");
            return;
        }

        // Verifying Server rebalancing states
        logger.info("[Post-Shrinkage] Post shrinkage Checking server states on all nodes");
        for(Node node: cluster.getNodes()) {
            Integer nodeId = node.getId();

            logger.info("Checking Node " + nodeId);
            Versioned<String> stateVersioned = adminClient.metadataMgmtOps.getRemoteMetadata(nodeId, MetadataStore.SERVER_STATE_KEY);
            if(stateVersioned == null) {
                logger.error("State object is null");
                shouldAbort = true;
            } else {
                if(!stateVersioned.getValue().equals("NORMAL_SERVER")) {
                    logger.error("Server state is not normal");
                    shouldAbort = true;
                }
            }
        }
        logger.info("[Post-Shrinkage] Finished checking server states on all nodes");
        if(shouldAbort) {
            logger.info("Aborting");
        }
    }
}
