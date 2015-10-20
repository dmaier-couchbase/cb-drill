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
import com.couchbase.apache.drill.config.CouchbaseConfig;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.log4j.Logger;

/**
 * One Group scan has multiple sub-scans
 * 
 * 
 * @author David Maier <david.maier at couchbase.com>
 */
@JsonTypeName("couchbase-subscan")
public class CBDefaultSubScan extends AbstractBase implements SubScan {

    private static final Logger LOG = Logger.getLogger(CBDefaultSubScan.class.getName());
    
     /**
     * The scan spec of the group scan
     */
    protected final CBDefaultScanSpec scanSpec;
    
    
    /**
     * The plugin config
     */
    protected final CBStoragePluginConfig config;
    
    
    /**
     * The associated storage plugin
     */
    protected final CBStoragePlugin plugin;
    
    
    //-- For debugging
    protected final String bucketName;

    /**
     * Construct from JSON
     * 
     * @param userName
     * @param scanSpec
     * @param config
     * @param pluginRegistry
     * @throws ExecutionSetupException 
     */
    @JsonCreator
    public CBDefaultSubScan(
            
            @JsonProperty("userName") String userName, 
            @JsonProperty("cbScanSpec") CBDefaultScanSpec scanSpec,
            @JsonProperty("storage") StoragePluginConfig config,
            @JacksonInject StoragePluginRegistry pluginRegistry ) throws ExecutionSetupException {
        
        this(userName, (CBStoragePlugin) pluginRegistry.getPlugin(config), scanSpec);
       
    }
    
    /**
     * The short constructor
     * 
     * @param userName
     * @param plugin
     * @param scanSpec 
     */
    public CBDefaultSubScan(String userName, CBStoragePlugin plugin, CBDefaultScanSpec scanSpec)
    {
        super(userName);
        
       LOG.debug("Initializing SubScan ...");
       
       this.plugin = plugin;
       this.scanSpec = scanSpec;
       this.config = (CBStoragePluginConfig) plugin.getConfig();
       this.bucketName = config.getConfig().get(CouchbaseConfig.CB_BUCKET);
           
       LOG.debug("userName = " + userName);
       LOG.debug("bucketName = " + bucketName);
       LOG.debug("plugin = " + plugin);
       LOG.debug("scanSpec = " + scanSpec); 
        
    }
    
    /**
     * The copy constructor
     * 
     * @param that 
     */
    private CBDefaultSubScan(CBDefaultSubScan that) {
        
        super(that);
        this.scanSpec = that.scanSpec;
        this.config = that.config;
        this.bucketName = that.bucketName;
        this.plugin = that.plugin;
    }
    
    
    /**
     * Quite every imlementation seems to use this default here
     * 
     * @param <T>
     * @param <X>
     * @param <E>
     * @param physicalVisitor
     * @param value
     * @return
     * @throws E
     */
    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
        
        return physicalVisitor.visitSubScan(this, value);
    }

    /**
     * Children are not supported, just return a new instance of the 
     * sub-scan
     * 
     * @param children
     * @return
     * @throws ExecutionSetupException 
     */
    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {

        if (!children.isEmpty()) throw new IllegalArgumentException("Childs are not supported!");
        
        return new CBDefaultSubScan(this);
    }

    /**
     * The type as integer of our Couchbase subscan operator
     * 
     * @return 
     */
    @Override
    public int getOperatorType() {
       
        return 10000 + 'c' + 'o' + 'u' + 'c' + 'h';
    }

    /**
     * Provide by default an empty one
     * 
     * @return 
     */
    @Override
    public Iterator<PhysicalOperator> iterator() {
       
        return new ArrayList<PhysicalOperator>().iterator();
    }    

    @Override
    public boolean isExecutable() {
        
       return false;
    }

    @JsonProperty("storage")
    public CBStoragePluginConfig getConfig() {
        return config;
    }

    @JsonProperty("cbScanSpec")
    public CBDefaultScanSpec getScanSpec() {
        return scanSpec;
    }
    
    
    
}
