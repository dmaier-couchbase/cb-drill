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

import com.couchbase.apache.drill.CBStoragePlugin;
import java.io.IOException;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;

/**
 *
 * @author David Maier <david.maier at couchbase.com>
 */
public class CBSchemaFactory implements SchemaFactory {

    /**
     * The name
     */
    private final String name;
    
    /**
     * The associated plug-in
     */
    private final CBStoragePlugin plugin;

    /**
     * The constructor
     * 
     * @param plugin
     * @param name 
     */
    public CBSchemaFactory(CBStoragePlugin plugin, String name) {
        this.name = name;
        this.plugin = plugin;
    }
    
    /**
     * Register a schema
     * 
     * @param schemaConfig
     * @param parent
     * @throws IOException 
     */
    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        
        CBSchema schema = new CBSchema(plugin, name);
        SchemaPlus plus = parent.add(name, schema);
        schema.setSchemaPlus(plus);
    }
    
}
