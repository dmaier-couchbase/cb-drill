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
import com.couchbase.apache.drill.conn.BucketFactory;
import com.couchbase.client.java.Bucket;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.log4j.Logger;

/**
 * A GroupScan operator represents all data which will be scanned by a given physical
 * plan.  It is the superset of all SubScans for the plan.
 * 
 * Each subscan works on a partition of the data. So one GroupScan has one or many subscans.
 * So the purpose of this class seems to be to create 1 or many subscans.
 * 
 * I would say that this operator 
 * 
 *  Multiple scan implementations would be possible:
 * 
 * * DCP Mutation scan (Group per node/vBucket)
 * * N1QL Primary Index scan (Don't group at all - SELECT * FROM bucket)
 * * N1QL Secondary Index scan (Group per GSI)
 * 
 * @author David Maier <david.maier at couchbase.com>
 */
@JsonTypeName("couchbase-scan")
public class CBDefaultGroupScan extends AbstractGroupScan {

    private static final Logger LOG = Logger.getLogger(CBDefaultGroupScan.class.getName());

    
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
    
    
    //-- For debugging purpose
    
    /**
     * The endpoint to use
     */
    protected String endpoint;
    
    /**
     * The bucket to use
     */
    protected final Bucket bucket;
    
    /**
     * The bucket to scan
     */
    protected final String bucketName;
    
    
    /**
     * Instantiate based on JSON
     * 
     * @param userName
     * @param scanSpec
     * @param config
     * @param pluginRegistry
     * @throws ExecutionSetupException 
     */
    @JsonCreator
    public CBDefaultGroupScan(
            @JsonProperty("userName") String userName,
            @JsonProperty("cbScanSpec") CBDefaultScanSpec scanSpec,
            @JsonProperty("storage") CBStoragePluginConfig config,
            @JacksonInject StoragePluginRegistry pluginRegistry ) throws ExecutionSetupException
    {
        
      this(userName, (CBStoragePlugin) pluginRegistry.getPlugin(config), scanSpec);
      
    }
    
    public CBDefaultGroupScan(String userName, CBStoragePlugin plugin, CBDefaultScanSpec scanSpec) {
        
        super(userName);
        
       LOG.debug("Initializing GroupScan ...");
       
       this.plugin = plugin;
       this.scanSpec = scanSpec;
       this.config = (CBStoragePluginConfig) plugin.getConfig();
       this.bucketName = config.getConfig().get(CouchbaseConfig.CB_BUCKET);
           
       LOG.debug("userName = " + userName);
       LOG.debug("bucketName = " + bucketName);
       LOG.debug("plugin = " + plugin);
       LOG.debug("scanSpec = " + scanSpec); 

       LOG.debug("Ensuring that the bucket connection is available");
       this.bucket = BucketFactory.getBucket();
        
    }
     
    
    /**
     * Copy constructor
     * 
     * @param that 
     */
    private CBDefaultGroupScan(CBDefaultGroupScan that)
    {
        super(that);
        
        this.bucketName = that.bucketName;
        this.bucket = that.bucket;
        this.scanSpec = that.scanSpec;
        this.plugin = that.plugin; 
        this.config = that.config;
    }  

    
    /**
     * We only support an empty list of children here by returning a new group
     * scan based on this one
     * 
     * @param children
     * @return
     * @throws ExecutionSetupException 
     */
    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
       
        if (!children.isEmpty()) throw new IllegalArgumentException("Child operators are not supported!");
        
        return new CBDefaultGroupScan(this);
    }

    /**
     * The idea is to split the work up into fragments by executing against multiple
     * endpoints.
     * 
     * We will just allow one endpoint here for now.
     * 
     * TODO: Later a scan could be performed by using a Global Secondary Index. So one sub-scan
     * per index would be an idea. We would need to ask the index directly in
     * this case.
     * 
     * @param endpoints
     * @throws PhysicalOperatorSetupException 
     */
    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
        
        LOG.debug("Applying assignments ...");
        LOG.debug("numOfEndpoints = " + endpoints.size());
        
        if (endpoints.size() == 1)
        {
            //TODO: It's not yet clear where this will be used because because I
            //passed the node list as part of the config
            this.endpoint = endpoints.get(0).getAddress();
            LOG.debug(endpoint);

        }
        else
        {
            throw new IllegalArgumentException("Wromg number of endpoints!");
        } 
    }

    
    /**
     * This would be the number of fragments to scan in parallel
     * 
     * We will allow just 1 for now
     * 
     * TODO: Maybe the number of existent GSI-s?
     * 
     * @return 
     */
    @Override
    public int getMaxParallelizationWidth() {
        
       return 1;  
    }
  
    @Override
    public String getDigest() {
        
        Map<String, String> groupScanProps = new HashMap<>();
        
        groupScanProps.put("class", CBDefaultGroupScan.class.getName());
        groupScanProps.put("bucket", this.bucketName);
        groupScanProps.put("scanSpec", this.scanSpec.toString());
        
        return groupScanProps.toString();
    }
    
    /**
     * Get a sub scan based on the fragement id
     * 
     * We currently just support one fragment here, so let's just return a
     * 
     * 
     * @param minorFragmentId
     * @return
     * @throws ExecutionSetupException 
     */
    @Override
    public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
        
        return new CBDefaultSubScan(getUserName(), plugin, scanSpec);
    }

    
    @JsonProperty("cbScanSpec")
    public CBDefaultScanSpec getScanSpec() {
        return scanSpec;
    }

    @JsonProperty("storage")
    public CBStoragePluginConfig getConfig() {
        return config;
    }   
     
    
}
