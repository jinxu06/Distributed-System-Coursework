package edinburgh.s1673820.ds.partC;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;


/**
 * Created by Jin Xu on 11/12/16.
 *
 * Class CacheConfig here define the configuration of cache to tore wikipedia data
 * Store type is defined here
 * There is no sliding window here. All data will remain in cache
 */


public class CacheConfig {
    public static CacheConfiguration<String, Long> wikiCache() {
        // new cache configuration named wiki
        CacheConfiguration<String, Long> cfg = new CacheConfiguration<>("wiki");
        // set transactional atomicity
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        // key type String, value type Long
        cfg.setIndexedTypes(String.class, Long.class);
        return cfg;
    }
}
