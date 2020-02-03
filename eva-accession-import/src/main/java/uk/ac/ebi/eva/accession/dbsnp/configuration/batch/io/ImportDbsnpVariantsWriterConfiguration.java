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
package uk.ac.ebi.eva.accession.dbsnp.configuration.batch.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.batch.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpSubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpSubmittedVariantOperationRepository;
import uk.ac.ebi.eva.accession.dbsnp.batch.io.DbsnpVariantsWriter;
import uk.ac.ebi.eva.accession.dbsnp.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.DBSNP_VARIANT_WRITER;

@Configuration
@Import({MongoConfiguration.class})
public class ImportDbsnpVariantsWriterConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ImportDbsnpVariantsWriterConfiguration.class);

    @Bean(name = DBSNP_VARIANT_WRITER)
    @StepScope
    DbsnpVariantsWriter dbsnpVariantWriter(InputParameters parameters, MongoTemplate mongoTemplate,
                                           ImportCounts importCounts,
                                           DbsnpSubmittedVariantOperationRepository operationRepository,
                                           DbsnpSubmittedVariantAccessioningRepository submittedVariantRepository,
                                           DbsnpClusteredVariantOperationRepository clusteredOperationRepository,
                                           DbsnpClusteredVariantAccessioningRepository clusteredVariantRepository)
            throws Exception {
        logger.info("Injecting dbsnpVariantWriter with parameters: {}", parameters);
        return new DbsnpVariantsWriter(mongoTemplate, operationRepository, submittedVariantRepository,
                                       clusteredOperationRepository, clusteredVariantRepository, importCounts);
    }
}
