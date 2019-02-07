/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.core.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@ConfigurationProperties(prefix = "dbsnp.datasource")
public class DbsnpDataSource implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(DbsnpDataSource.class);

    private String url;

    @Field("driver-class-name")
    private String driverClassName;

    private String username;

    private String password;

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.debug("Properties were set to: {}", this);
    }

    public DataSource getDatasource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        return dataSource;
    }

    public String getUrl() {
        return url;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        return "DbsnpDatasource{" +
                "url='" + url + '\'' +
                ", driverClassName='" + driverClassName + '\'' +
                '}';
    }
}
