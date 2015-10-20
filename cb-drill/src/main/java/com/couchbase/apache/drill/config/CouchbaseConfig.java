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

import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author David Maier <david.maier at couchbase.com>
 */
public class CouchbaseConfig extends BaseConfig {

    private static final String FILE_NAME = "cb.properties";
    

    // Configuration properties
    public static final String CB_HOSTS = "cb.con.hosts";
    public static final String CB_PORT = "cb.con.port";
    public static final String CB_BUCKET = "cb.con.bucket.name";
    public static final String CB_BUCKETPWD = "cb.con.bucket.pwd";
    public static final String CB_ADMINUSR =  "cb.con.admin.usr";
    public static final String CB_ADMINPWD = "cb.con.admin.pwd";
    public static final String[] PROPS = new String[]{CB_HOSTS, CB_PORT, CB_BUCKET, 
                                                CB_BUCKETPWD, CB_ADMINUSR, 
                                                CB_ADMINPWD};
    
    
    //Settings
    private String[] hosts;
    private int port;
    private String bucket;
    private String password;
    private String adminUser;
    private String adminPassword;
       
    /**
     * The constructor which loads by default from the properties file
     * @throws IOException 
     */
    public CouchbaseConfig() throws IOException {
        
        super(FILE_NAME);
        init();
        
    }
    
    /**
     * The constructor which loads from another properties file
     * @param props 
     */
    public CouchbaseConfig(Properties props) {
        
        super(props);
        init();
    }
    
    /**
     * Initialize the settings
     */
    private void init() {
        
        this.hosts = this.props.getProperty(CB_HOSTS).split(",");
        this.port = Integer.parseInt(this.props.getProperty(CB_PORT));
        this.bucket = props.getProperty(CB_BUCKET);
        this.password = this.props.getProperty(CB_BUCKETPWD);
        this.adminUser = props.getProperty(CB_ADMINUSR);
        this.adminPassword =  props.getProperty(CB_ADMINPWD);
    }
    

    public String[] getHosts() {
      
        return this.hosts;
    }
    
    public int getPort() {
       
        return this.port;
    }
    
    public String getBucket() {
    
        return this.bucket;
    }
    
    public String getPassword() {

        return this.password;
    }

    public String getAdminUser() {
        return this.adminUser;
    }

    public String getAdminPassword() {
        return this.adminPassword;
    }  
}
