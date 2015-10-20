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

package com.couchbase.apache.drill;

import com.couchbase.apache.drill.config.CBStoragePluginConfig;
import com.couchbase.apache.drill.schema.CBSchemaFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.log4j.Logger;

/**
 * This is the Couchbase storage plug-in
 * 
 * @author David Maier <david.maier at couchbase.com>
 */
public class CBStoragePlugin extends AbstractStoragePlugin {

    private static final Logger LOG = Logger.getLogger(CBStoragePlugin.class.getName());
    
    
    /**
     * The Couchbase plug-in configuration
     */
    private final CBStoragePluginConfig config;
    
    /**
     * The Couchbase schema factory
     */
    private final CBSchemaFactory schemaFactory;
    
    /**
     * The drill context
     */
    private final DrillbitContext context;
   
    
    /**
     * The Constructor
     * @param config
     * @param context
     * @param name
     */
    public CBStoragePlugin(CBStoragePluginConfig config, DrillbitContext context, String name) {
        
        LOG.debug("Initializing Couchbase Storage Plugin ...");
        LOG.debug("context = " + context.toString());
        LOG.debug("name = " + name);
        
        this.config = config;
        this.context = context;
        this.schemaFactory = new CBSchemaFactory(this, name);        
    }
    
    
    /**
     * Gets the configuration
     * 
     * @return 
     */
    @Override
    public StoragePluginConfig getConfig() {  
        return this.config;
    }

    /**
     * Gets the passed context
     * 
     * @return 
     */
    public DrillbitContext getContext() {
        return this.context;
    }
    
    
    /**
     * Register the schema
     * 
     * @param schemaConfig
     * @param parent
     * @throws IOException 
     */
    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        
        LOG.debug("Registering schemas ...");
        LOG.debug("schemaConfig = " + schemaConfig.toString());
        LOG.debug("parent = " + parent.toString());
        
        this.schemaFactory.registerSchemas(schemaConfig, parent);     
    }

    /**
     * Indicates that the Couchbase plug-in does support reads
     * 
     * @return 
     */
    @Override
    public boolean supportsRead() {
        return true;
    }

    /**
     * Indicates that the Couchbase plug-in does support writes
     * 
     * @return 
     */
    @Override
    public boolean supportsWrite() {
       return false;
    }

    @Override
    public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
       
        LOG.debug("Retrieving physical scan ...");
        LOG.debug("userName = " + userName);
        LOG.debug("selection = " + selection.toString());
         
        CBScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<CBScanSpec>(){});
        
        return new CBGroupScan(userName, this, scanSpec, null);
    }

    
    
    
    
}
