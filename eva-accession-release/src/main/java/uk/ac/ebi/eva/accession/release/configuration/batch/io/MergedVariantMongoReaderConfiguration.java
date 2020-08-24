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

package uk.ac.ebi.eva.accession.release.configuration.batch.io;

import com.mongodb.MongoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.release.batch.io.merged.MergedVariantMongoReader;
import uk.ac.ebi.eva.accession.release.collectionNames.DbsnpCollectionNames;
import uk.ac.ebi.eva.accession.release.collectionNames.EvaCollectionNames;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.commons.batch.io.UnwindingItemStreamReader;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_MERGED_VARIANT_READER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_MERGED_VARIANT_READER;

@Configuration
@Import({MongoConfiguration.class})
public class MergedVariantMongoReaderConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(MergedVariantMongoReaderConfiguration.class);

    @Bean(DBSNP_MERGED_VARIANT_READER)
    @StepScope
    public ItemStreamReader<Variant> unwindingReaderDbsnp(InputParameters parameters, MongoClient mongoClient,
                                                     MongoProperties mongoProperties) {
        logger.info("Injecting Dbsnp MergedVariantMongoReader with parameters: {}", parameters);
        return new UnwindingItemStreamReader<>(
                new MergedVariantMongoReader(parameters.getAssemblyAccession(), mongoClient,
                                             mongoProperties.getDatabase(), parameters.getChunkSize(),
                                             new DbsnpCollectionNames()));
    }

    @Bean(EVA_MERGED_VARIANT_READER)
    @StepScope
    public ItemStreamReader<Variant> unwindingReaderEva(InputParameters parameters, MongoClient mongoClient,
                                                     MongoProperties mongoProperties) {
        logger.info("Injecting Eva MergedVariantMongoReader with parameters: {}", parameters);
        return new UnwindingItemStreamReader<>(
                new MergedVariantMongoReader(parameters.getAssemblyAccession(), mongoClient,
                                             mongoProperties.getDatabase(), parameters.getChunkSize(),
                                             new EvaCollectionNames()));
    }

}
