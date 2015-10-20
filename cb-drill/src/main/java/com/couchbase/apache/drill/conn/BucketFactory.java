 /*
  * Copyright 2015 Couchbase, Inc.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *      http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package com.couchbase.apache.drill.conn;

import com.couchbase.apache.drill.config.ConfigFactory;
import com.couchbase.apache.drill.config.CouchbaseConfig;
import com.couchbase.apache.drill.error.BucketConnectError;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import org.apache.log4j.Logger;


/**
 *
 * @author David Maier <david.maier at couchbase.com>
 */
public class BucketFactory {

    private static final Logger LOG = Logger.getLogger(BucketFactory.class.getName());
    
    private static Bucket bucket;
    
    public static Bucket getBucket()
    {
    
        if (bucket == null)
            createBucketCon();
        
        return bucket;
    }
    
    public static AsyncBucket getAsyncBucket()
    {
    
        if (bucket == null)
            createBucketCon();
        
        return bucket.async();
    }
    
    
    
    /**
     * Create the bucket connection based on the configuration
     * 
     * @return 
     */
    public static Bucket createBucketCon()
    {
        
        CouchbaseConfig cfg = ConfigFactory.getCBConfig();
        Cluster cluster = ClusterFactory.getCluster();
       
        
        try {

            if (cfg.getPassword() != null && !cfg.getPassword().equals("")) {
                bucket = cluster.openBucket(cfg.getBucket(), cfg.getPassword());
            } else {
                bucket = cluster.openBucket(cfg.getBucket());
            }

        } catch (RuntimeException e) {
            BucketConnectError err = new BucketConnectError(e, cfg.getBucket());
            LOG.error(err.toString());
            throw err;
        }
        
       
        return bucket;
    }
}
