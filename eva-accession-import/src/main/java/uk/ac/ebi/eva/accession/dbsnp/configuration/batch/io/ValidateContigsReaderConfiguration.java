/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.dbsnp.configuration.batch.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.core.configuration.DbsnpDataSource;
import uk.ac.ebi.eva.accession.dbsnp.batch.io.SubSnpNoHgvsContigAndChromosomeReader;
import uk.ac.ebi.eva.accession.dbsnp.parameters.InputParameters;

import javax.sql.DataSource;

import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.CONTIG_AND_CHROMOSOME_READER;

@Configuration
@EnableConfigurationProperties({DbsnpDataSource.class})
public class ValidateContigsReaderConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ValidateContigsReaderConfiguration.class);

    @Bean(name = CONTIG_AND_CHROMOSOME_READER)
    @StepScope
    SubSnpNoHgvsContigAndChromosomeReader subSnpNoHgvsContigAndChromosomeReader(InputParameters parameters,
                                                                                DbsnpDataSource dbsnpDataSource)
            throws Exception {
        logger.info("Injecting SubSnpNoHgvsContigAndChromosomeReader with parameters: {}, {}", parameters,
                    dbsnpDataSource);
        DataSource dataSource = dbsnpDataSource.getDatasource();
        return new SubSnpNoHgvsContigAndChromosomeReader(parameters.getAssemblyName(), parameters.getBuildNumber(),
                                                         dataSource, parameters.getPageSize());
    }
}
