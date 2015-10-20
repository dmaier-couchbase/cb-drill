
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

import com.couchbase.apache.drill.error.ConfigReadError;
import java.io.IOException;
import java.util.Properties;
import org.apache.log4j.Logger;

/**
 * To access the configurations
 *
 * @author David Maier <david.maier at couchbase.com>
 */
public class ConfigFactory {

    private static CouchbaseConfig cbConfig;

    private static final Logger LOG = Logger.getLogger(ConfigFactory.class.getName());

    /**
     * Create the Couchbase config based on optional properties
     *
     * @param props
     * @return
     */
    public static CouchbaseConfig create(Properties props) {

        try {

            if (props == null) {

                cbConfig = new CouchbaseConfig();

            } else {
                cbConfig = new CouchbaseConfig(props);
            }

        } catch (IOException ex) {

            ConfigReadError err = new ConfigReadError(ex);
            LOG.error(err.toString());
            throw err;
        }

        return cbConfig;

    }

    /**
     * Get the Couchbase config or create it based on optional propeties if it
     * was not yet created
     *
     * @param props
     * @return
     */
    public static CouchbaseConfig getCBConfig(Properties props) {

        if (cbConfig == null) {

            create(props);

        }

        return cbConfig;
    }

    /**
     * Get the Couchbase config or create it based on the config file if was not
     * yet created
     *
     * @return
     */
    public static CouchbaseConfig getCBConfig() {

        return getCBConfig(null);
    }

}
