package edinburgh.s1673820.ds.partC;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.File;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.IgniteCompute;

/**
 * Created by Aaron-MAC on 14/11/2016.
 *
 * Query node is defined here
 * query wikiCache for the top 10 popular pages
 * the query is executed very 10 seconds
 * query node is in client mode and belong to QueryNode cluster group
 */
public class QueryPages {

    /**
     * class PoisonPill (Groovy code)
     * http://apache-ignite-users.70518.x6.nabble.com/How-shutdown-all-nodes-td1531.html
     * used to send termination signal to all nodes in the cluster group
     */
    static class PoisonPill implements IgniteRunnable {
        @IgniteInstanceResource
        private transient Ignite ignite;
        @Override
        public void run() {
            new Thread() {
                @Override public void run() {
                    ignite.close();
                }
            }.start();
        }
    }

    public static void main(String[] args) throws Exception {
        // Mark this cluster member as client.
        Ignition.setClientMode(true);
        // start query node with query configuration
        try (Ignite ignite = Ignition.start("config/ignite-query-config.xml")) {
            IgniteCache<String, Long> stmCache = ignite.getOrCreateCache(CacheConfig.wikiCache());
            // SQL query, find the top 10 with the biggest value (view count), and sorted in ​descending order
            SqlFieldsQuery top10Qry = new SqlFieldsQuery(
                    "select _key, _val from Long order by _val desc limit 10");
            Thread.sleep(10000);
            while (true) {
                // write to output fine, append mode
                PrintWriter writer = new PrintWriter(new FileOutputStream(new File("../../log/log-partC.txt"), true));
                // Execute queries.
                List<List<?>> top10 = stmCache.query(top10Qry).getAll();
                // get unix time
                Long timeWhenQry = System.currentTimeMillis() / 1000L;

                for (List<?> line : top10) {
                    // string format
                    String output = timeWhenQry + ":" + line.get(1) + ":" + line.get(0);
                    // output to console
                    System.out.println(output);
                    // output to log file
                    writer.println(output);
                }
                writer.close();

                // StreamGroup cluster group
                ClusterGroup streamGroup = ignite.cluster().forAttribute("ROLE", "StreamNode");
                try {
                    int numNode = streamGroup.metrics().getTotalNodes();
                    System.out.println("Streamer Left:"+numNode);
                }
                catch (ClusterGroupEmptyException e){
                    // if the StreamNode cluster group is empty then streaming has finished
                    // So broadcast to all CacheNode and tell then to shutdown
                    ClusterGroup cacheGroup = ignite.cluster().forAttribute("ROLE", "CacheNode");
                    IgniteCompute compute = ignite.compute(cacheGroup);
                    compute.broadcast(new PoisonPill());
                    // wait for 10 seconds and then shut down the query node
                    Thread.sleep(10000);
                    ignite.close();
                    break;
                }
                Thread.sleep(10000);
            }
        }
    }
}
