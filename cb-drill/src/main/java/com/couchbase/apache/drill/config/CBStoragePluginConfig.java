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
package com.couchbase.apache.drill.config;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.log4j.Logger;

/**
 * This is the Couchbase configuration for the plug-in
 * 
 * @author David Maier <david.maier at couchbase.com>
 */

@JsonTypeName(CBStoragePluginConfig.NAME)
public class CBStoragePluginConfig extends StoragePluginConfigBase {

    private static final Logger LOG = Logger.getLogger(CBStoragePluginConfig.class.getName());
    
    /**
     * The name of this configuration
     */
    public static final String NAME = "couchbase";
    
    /**
     * The inner config
     */
    private final Map<String, String> config;
    
    
    /**
     * Jackson's JsonCreator allows to istantiate from JSON
     * 
     * In this case our JSON object needs to have a 'config' property
     * 
     * @param settings 
     */
     @JsonCreator
    public CBStoragePluginConfig(@JsonProperty("config") Map<String, String> settings) {
    
        LOG.debug("Initializing Couchbase Storage Plugin Config ...");
        
         this.config = settings;

         if (settings != null) {

             LOG.debug("config = " + settings.toString());
             
             Properties props = new Properties();

             List<String> allowdProps = Arrays.asList(CouchbaseConfig.PROPS);

             settings.entrySet().stream().forEach((entrySet) -> {
                 String key = entrySet.getKey();
                 
                if (allowdProps.contains(key)) {
                    props.put(key, entrySet.getValue());
                }
            });

             ConfigFactory.create(props);
         } else {
             
             LOG.debug("No configuration settings were passed. Using defaults.");
             ConfigFactory.create(null);
         }
        
    }

    
    @JsonProperty
    public Map<String, String> getConfig() {
        return config;
    }

    
    /**
     * Check if another config is equal
     * @param o
     * @return 
     */
    @Override
    public boolean equals(Object o) {

        if (o instanceof CBStoragePluginConfig)
        {
            return config.equals(((CBStoragePluginConfig)o).config);
        }
        else
        {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 53 * hash + Objects.hashCode(this.config);
        return hash;
    }
     
}
