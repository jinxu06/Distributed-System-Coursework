package edinburgh.s1673820.ds.partA;

import org.apache.ignite.*;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.IgniteTransactions;

import java.io.*;
import java.io.FileInputStream;


/**
 * Created by Aaron-MAC on 11/12/16.
 *
 * class StreamPages
 * in client mode and belong to StreamNode cluster group
 */

public class StreamPages {


    public static void main(String[] args) throws Exception {
        // Mark this cluster member as client.
        Ignition.setClientMode(true);
        // start stream node use stream configuration
        try (Ignite ignite = Ignition.start("config/ignite-stream-config.xml")) {
            // get wiki cache
            IgniteCache<String, Long> stmCache = ignite.getOrCreateCache(CacheConfig.wikiCache());
            // Transaction is used here due to concurrent problem
            IgniteTransactions transactions = ignite.transactions();
            try (IgniteDataStreamer<String, Long> stmr = ignite.dataStreamer(stmCache.getName())) {
                // if entry key exsits, andd view count to the original value
                stmr.allowOverwrite(true);
                stmr.receiver(StreamTransformer.from((e, arg) -> {
                    // get current entry value
                    Long var = e.getValue();
                    // get new view count
                    Long hit = (Long) arg[0];
                    // add to the original value and set back
                    e.setValue(var == null ? hit : var + hit);
                    return null;
                }));

                // accept comand line argument and read log input
                FileInputStream in = new FileInputStream(args[0]);
                // read file line by line and add relative data to cache
                try (LineNumberReader rdr = new LineNumberReader(new InputStreamReader(in))) {
                    for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
                        // while space split
                        String[] items = line.split("\\s+");
                        // match all the wikipedia page (first item without period simbol)
                        if(items[0].matches("\\w+"))
                        {
                            Long hit = Long.parseLong(items[2]);
                            // add (key=pagename, value=viewcount) to cache
                            stmr.addData(items[1], hit);
                        }
                    }
                }

            }

        }
    }
}
