package voldemort;

import java.util.Iterator;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class DatabaseMigration {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("src-config", "Configuration of source voldemort server")
              .withRequiredArg()
              .describedAs("source server config")
              .ofType(String.class);
        parser.accepts("dst-config", "Configuration of destination voldemort server")
              .withRequiredArg()
              .describedAs("destination server config")
              .ofType(String.class);
        parser.accepts("stores", "Stores to migrate")
              .withRequiredArg()
              .describedAs("store-names")
              .withValuesSeparatedBy(',')
              .ofType(String.class);
        OptionSet options = parser.parse(args);

        // check options
        if(!options.has("stores") || !options.has("src-config") || !options.has("dst-config")) {
            Utils.croak("Options: --stores [store1[,store2[,store3...]]]"
                        + "         --src-config <path_to_src_config>"
                        + "         --dst-config <path_to_dst_config>");
        }

        // get config
        VoldemortConfig srcConfig = null;
        VoldemortConfig dstConfig = null;
        srcConfig = VoldemortConfig.loadFromVoldemortHome((String) options.valueOf("src-config"));
        dstConfig = VoldemortConfig.loadFromVoldemortHome((String) options.valueOf("dst-config"));

        // start servers
        final VoldemortServer srcServer = new VoldemortServer(srcConfig);
        if(!srcServer.isStarted())
            srcServer.start();
        final VoldemortServer dstServer = new VoldemortServer(dstConfig);
        if(!dstServer.isStarted())
            dstServer.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                if(srcServer.isStarted())
                    srcServer.stop();
                if(dstServer.isStarted())
                    dstServer.stop();
            }
        });

        // set up AdminClients
        String srcUrl = "tcp://localhost:6666/";
        String dstUrl = "tcp://localhost:7666/";
        AdminClient srcAdminClient = new AdminClient(srcUrl, new AdminClientConfig());
        AdminClient dstAdminClient = new AdminClient(dstUrl, new AdminClientConfig());

        // stream data
        List<Integer> partitionIdList = null;
        partitionIdList = Lists.newArrayList();
        for(Node node: srcAdminClient.getAdminClientCluster().getNodes()) {
            partitionIdList.addAll(node.getPartitionIds());
        }

        int nodeId = 0;
        Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator;
        List<String> storeNames = (List<String>) options.valuesOf("stores");
        for(String storeName: storeNames) {
            iterator = srcAdminClient.fetchEntries(nodeId, storeName, partitionIdList, null, false);
            dstAdminClient.updateEntries(nodeId, storeName, iterator, null);
        }
        srcServer.stop();
        dstServer.stop();
    }
}
