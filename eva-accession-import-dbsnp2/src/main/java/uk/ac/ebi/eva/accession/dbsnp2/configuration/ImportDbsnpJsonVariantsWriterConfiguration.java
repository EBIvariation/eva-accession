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
package uk.ac.ebi.eva.accession.dbsnp2.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.io.DbsnpClusteredVariantOperationWriter;
import uk.ac.ebi.eva.accession.core.io.DbsnpClusteredVariantWriter;
import uk.ac.ebi.eva.accession.core.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.dbsnp2.io.DbsnpJsonClusteredVariantsWriter;
import uk.ac.ebi.eva.accession.dbsnp2.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.DBSNP_JSON_VARIANT_WRITER;

@Configuration
@Import({MongoConfiguration.class})
public class ImportDbsnpJsonVariantsWriterConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ImportDbsnpJsonVariantsWriterConfiguration.class);

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private ImportCounts importCounts;

    @Bean(name = DBSNP_JSON_VARIANT_WRITER)
    @StepScope
    public ItemWriter<DbsnpClusteredVariantEntity> writer
            (InputParameters parameters,
             DbsnpClusteredVariantOperationRepository clusteredOperationRepository,
             DbsnpClusteredVariantAccessioningRepository clusteredVariantRepository) {
        logger.info("Injecting dbsnpClusteredVariantWriter with parameters: {}", parameters);
        DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter = new DbsnpClusteredVariantWriter(mongoTemplate,
                                                                                                  importCounts);
        DbsnpClusteredVariantOperationWriter dbsnpClusteredVariantOperationWriter =
                new DbsnpClusteredVariantOperationWriter(mongoTemplate, importCounts);
        return new DbsnpJsonClusteredVariantsWriter(dbsnpClusteredVariantWriter, dbsnpClusteredVariantOperationWriter,
                                                    clusteredOperationRepository, clusteredVariantRepository);
    }
}