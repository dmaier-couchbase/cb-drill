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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.log4j.Logger;

/**
 * Defines how a scan in Couchbase is looking like.
 * 
 * Our Couchbase scan is defined by using
 * the bucket to scan from and the N1QL query 
 * which should be used to gather results
 * 
 * @author David Maier <david.maier at couchbase.com>
 */
public class CBScanSpec {
    
    private static final Logger LOG = Logger.getLogger(CBScanSpec.class.getName());
    
    /**
     * The bucket to scan
     */
    private final String bucket;

    /**
     * The query to use
     * 
     * TODO: Find out if N1QL pass through would be possible
     */
    private String query = "";
    
    /**
     * Jackson allows to create an instance from JSON
     * by using a JsonCreator
     * 
     * @param bucket
     * @param query 
     */
    @JsonCreator
    public CBScanSpec(@JsonProperty("bucket") String bucket, @JsonProperty("query") String query) {
        
        LOG.debug("Initializing Couchbase ScanSpec ...");
        LOG.debug("query = " + query);
        LOG.debug("bucket = " + bucket);
        
        this.bucket = bucket;
        if (query != null) this.query = query;
    }

    public CBScanSpec(String bucket)
    {
        this.bucket = bucket;
    }
    
    public String getBucket() {
        return bucket;
    }

    public String getQuery() {
        return query;
    } 

    @Override
    public String toString() {
        
        return bucket + ": " + query;
    }
    
    
    
    
}
