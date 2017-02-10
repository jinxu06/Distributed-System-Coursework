package edinburgh.s1673820.ds.partB;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.IgniteException;

/**
 * Created by Jin Xu on 11/12/16.
 *
 * class IgniteServerNodeStarter is used to start normal data node in server node
 * The node is in server mode, and in CacheNode cluster group
 * No discovery mode is turn on
 */

public class IgniteServerNodeStarter
{
    public static void main( String[] args ) throws IgniteException {
        // set node to be a server node, so that it will participate in cache
        Ignition.setClientMode(false);
        // start ignite node with cache node configuration (no discovery)
        Ignite ignite = Ignition.start("config/ignite-cache-config.xml");
    }
}
