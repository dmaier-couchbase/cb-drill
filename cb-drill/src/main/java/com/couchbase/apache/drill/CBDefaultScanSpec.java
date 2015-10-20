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
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * Defines how a scan in Couchbase is looking like. 
 * 
 * Our Couchbase scan is defined by defining which bucket is used
 * 
 * @author David Maier <david.maier at couchbase.com>
 */
public class CBDefaultScanSpec {
    
    private static final Logger LOG = Logger.getLogger(CBDefaultScanSpec.class.getName());
    
    /**
     * The bucket to scan
     */
    protected String bucket;
    
    /**
     * Jackson allows to create an instance from JSON
     * by using a JsonCreator
     * 
     * @param bucket 
     */
    @JsonCreator
    public CBDefaultScanSpec(@JsonProperty("bucket") String bucket) {
        
        LOG.debug("Initializing Couchbase ScanSpec ...");
        LOG.debug("bucket = " + bucket);
        
        this.bucket = bucket;
    }

    
    public String getBucket() {
        return bucket;
    }


    @Override
    public String toString() {
        
        Map<String,String> specProps = new HashMap<>();
        specProps.put("bucket", bucket);
        
        return specProps.toString();
    }
    
}
