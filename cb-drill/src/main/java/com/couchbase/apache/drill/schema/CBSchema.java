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
package com.couchbase.apache.drill.schema;

import com.couchbase.apache.drill.CBScanSpec;
import com.couchbase.apache.drill.CBStoragePlugin;
import com.couchbase.apache.drill.config.CBStoragePluginConfig;
import com.couchbase.apache.drill.config.ConfigFactory;
import com.couchbase.apache.drill.conn.ClusterFactory;
import com.couchbase.apache.drill.error.BucketListRetrievalError;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.log4j.Logger;

/**
 *
 * @author David Maier <david.maier at couchbase.com>
 */
public class CBSchema extends AbstractSchema { 
    
    private final static Logger LOG = Logger.getLogger(CBSchema.class.getName());
    
    /**
     * N1QL is using buckets like tables, so let's map a bucket to a table here
     */
    private final Set<String> buckets = new HashSet<>();
    
    /**
     * Seems to be a kind of schema definition
     */
    private SchemaPlus schemaPlus;
    
    /**
     * The associated plugin
     */
    private final CBStoragePlugin plugin;
    
  
    /**
     * Required constructor
     * 
     * @param plugin
     * @param name 
     */
    public CBSchema(CBStoragePlugin plugin, String name) {
        
        //No parent schemas
        super(new ArrayList<>(), name);
        
        this.plugin = plugin;
    }    
    
    /**
     * The type of this schema
     * 
     * @return 
     */
    @Override
    public String getTypeName() {
    
        return CBStoragePluginConfig.NAME;
    }

    /**
     * No sub-schemas
     * 
     * @return 
     */
    @Override
    public Set<String> getSubSchemaNames() {
        
        //So return an empty map
        return new HashSet<>();
    }

    /**
     * No sub-schema
     * 
     * @param name
     * @return 
     */
    @Override
    public AbstractSchema getSubSchema(String name) {
    
        //Return null to indicate that this sub-schema is not existent
        return null;
    }

    /**
     * Get the table names. We map tables to buckets in our case.
     * 
     * @return 
     */
    @Override
    public Set<String> getTableNames() {
       
        LOG.debug("Retrieving bucket names ...");
        
        try {

            ClusterManager manager = ClusterFactory.getCluster()
                    .clusterManager(ConfigFactory.getCBConfig()
                            .getAdminUser(), ConfigFactory.getCBConfig().getAdminPassword());

            List<BucketSettings> bucketSettings = manager.getBuckets();

            bucketSettings.stream().forEach((bs) -> {
                this.buckets.add(bs.name());
            });

        } catch (RuntimeException e) {

            BucketListRetrievalError err = new BucketListRetrievalError(e);
            LOG.error(err.toString());
            throw err;
        }

        return this.buckets;
    }

    /**
     * Set the schema plus
     * 
     * TODO: Seems to be a kind of parent in the schema tree
     * 
     * @param schemaPlus 
     */
    public void setSchemaPlus(SchemaPlus schemaPlus) {
        this.schemaPlus = schemaPlus;
    }

    public SchemaPlus getSchemaPlus() {
        return schemaPlus;
    }

    public CBStoragePlugin getPlugin() {
        return plugin;
    }
      
 
    
    /**
     * Get a specific table
     * 
     * @param name
     * @return 
     */
    @Override
    public Table getTable(String name) {
    
        return new DynamicDrillTable(plugin, name, new CBScanSpec(name));
    }  
}
