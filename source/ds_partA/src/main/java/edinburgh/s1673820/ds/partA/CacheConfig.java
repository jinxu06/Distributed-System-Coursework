package edinburgh.s1673820.ds.partA;

import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.configuration.FactoryBuilder;
import org.apache.ignite.configuration.CacheConfiguration;


/**
 * Created by Jin Xu on 11/12/16.
 *
 * Class CacheConfig here define the configuration of cache to store wikipedia data
 * Sliding window and storage type are set here
 */


public class CacheConfig {
    public static CacheConfiguration<String, Long> wikiCache() {
        // new cache configuration named wiki
        CacheConfiguration<String, Long> cfg = new CacheConfiguration<>("wiki");
        // key type String, value type Long
        cfg.setIndexedTypes(String.class, Long.class);
        // Sliding window of 1 seconds.
        cfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(
                new CreatedExpiryPolicy(new Duration(TimeUnit.SECONDS, 1))));
        return cfg;
    }
}
