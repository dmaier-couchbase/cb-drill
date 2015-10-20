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
import com.couchbase.apache.drill.error.ClusterRefCreationError;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author David Maier <david.maier at couchbase.com>
 */
public class ClusterFactory {
    
    private static final Logger LOG = Logger.getLogger(ClusterFactory.class.getName());
    
    private static Cluster cluster;
    
    
    public static Cluster getCluster()
    {
        if (cluster == null)
            createCluster();
       
        return cluster;
    }
    
    /**
     * Create a cluster reference based on the configuration
     * 
     * @return 
     */
    public static Cluster createCluster()
    {
        

        //Create the cluster reference
        String[] hosts = ConfigFactory.getCBConfig().getHosts();
        List<String> nodes = Arrays.asList(hosts);
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().build();

        try {

            cluster = CouchbaseCluster.create(env, nodes);
        
        } catch (RuntimeException e) {
            
            ClusterRefCreationError err = new ClusterRefCreationError(e, hosts);
            LOG.error(err.toString());
            
        }
        
        
        return cluster;
    }
}
