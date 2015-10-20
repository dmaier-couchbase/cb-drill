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
import com.couchbase.apache.drill.conn.BucketFactory;
import com.couchbase.client.java.Bucket;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.List;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.StoragePluginRegistry;

/**
 * TODO: Implement the abstract methods
 * 
 * @author David Maier <david.maier at couchbase.com>
 */
@JsonTypeName("couchbase-scan")
public class CBGroupScan extends AbstractGroupScan {

    /**
     * The scan spec of the group scan
     */
    private final CBScanSpec scanSpec;
    
    /**
     * The columns
     */
    private final List<SchemaPath> columns;
    
    
    /**
     * The associated storage plugin
     */
    private final CBStoragePlugin plugin;
    
    
    /**
     * The bucket to use
     */
    private final Bucket bucket;
    
    /**
     * Instantiate based on JSON
     * 
     * @param userName
     * @param scanSpec
     * @param config
     * @param columns
     * @param pluginRegistry
     * @throws ExecutionSetupException 
     */
    @JsonCreator
    public CBGroupScan(
            @JsonProperty("userName") String userName,
            @JsonProperty("cbScanSpec") CBScanSpec scanSpec,
            @JsonProperty("storage") CBStoragePluginConfig config,
            @JsonProperty("columns") List<SchemaPath> columns,
            @JacksonInject StoragePluginRegistry pluginRegistry ) throws ExecutionSetupException
    {
        
       this(userName, (CBStoragePlugin) pluginRegistry.getPlugin(config), scanSpec, columns);
        
    }
    
    /**
     * The short constructor
     * 
     * @param userName
     * @param plugin
     * @param scanSpec
     * @param columns 
     */
    public CBGroupScan(String userName, CBStoragePlugin plugin, 
                        CBScanSpec scanSpec, List<SchemaPath> columns)
    {
        super(userName);
        this.plugin = plugin;
        this.scanSpec = scanSpec;

        
        if (columns == null || columns.isEmpty())
            this.columns = ALL_COLUMNS;
        else 
            this.columns = columns;
        
        
        this.bucket = BucketFactory.getBucket();
    }
    
    
    
    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getMaxParallelizationWidth() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getDigest() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
