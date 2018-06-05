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
package uk.ac.ebi.eva.accession.dbsnp.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.init.DatabasePopulator;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;

@ConfigurationProperties(prefix = "dbsnp.datasource")
public class DbsnpTestDataSource implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(DbsnpTestDataSource.class);

    private static boolean initialized = false;

    private DbsnpDataSource dbsnpDatasource;

    private String schema;

    private String data;

    public DbsnpTestDataSource(DbsnpDataSource dbsnpDatasource) {
        this.dbsnpDatasource = dbsnpDatasource;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        populateDatabase();
    }

    public void populateDatabase() {
        if (!initialized) {
            logger.info("Populating test DB");
            DatabasePopulatorUtils.execute(databasePopulator(), dbsnpDatasource.getDatasource());
        }
        initialized = true;
    }

    private DatabasePopulator databasePopulator() {
        final ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new FileSystemResource(schema));
        populator.addScript(new FileSystemResource(data));
        return populator;
    }

    public DataSource getDatasource() {
        return dbsnpDatasource.getDatasource();
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void setData(String data) {
        this.data = data;
    }
}