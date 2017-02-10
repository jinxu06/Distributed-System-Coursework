package edinburgh.s1673820.ds.partC;

import org.apache.ignite.*;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.cluster.ClusterGroup;

import java.io.*;
import java.io.FileInputStream;


/**
 * Created by Aaron-MAC on 11/12/16.
 */

public class StreamPages {


    public static void main(String[] args) throws Exception {
        // Mark this cluster member as client.
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("config/ignite-stream-config.xml")) {
            IgniteCache<String, Long> stmCache = ignite.getOrCreateCache(CacheConfig.wikiCache());
            IgniteTransactions transactions = ignite.transactions();
            try (IgniteDataStreamer<String, Long> stmr = ignite.dataStreamer(stmCache.getName())) {

                stmr.allowOverwrite(true);
                stmr.receiver(StreamTransformer.from((e, arg) -> {
                    try (Transaction tx = transactions.txStart()) {
                        Long var = e.getValue();
                        Long hit = (Long) arg[0];
                        e.setValue(var == null ? hit : var + hit);
                        tx.commit();
                    }
                    return null;

                }));

                //while (true) {
                // accept comand line argument and read log input
                // String fp = "/afs/inf.ed.ac.uk/user/s16/s1673820/Data/wiki/pagecounts-test";
                FileInputStream in = new FileInputStream(args[0]);
                try (LineNumberReader rdr = new LineNumberReader(new InputStreamReader(in))) {
                    for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
                        // while space split
                        String[] items = line.split("\\s+");
                        // match all the wikipedia page (first item without period simbol)
                        if(items[0].matches("\\w+\\.b"))
                        {
                            Long hit = Long.parseLong(items[2]);
                            // add (key=pagename, value=viewcount) to cache
                            stmr.addData(items[1], hit);
                        }
                    }

                }
                //}

            }

        }







    }
}
