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
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.release.batch.io.merged_deprecated.DbsnpMergedDeprecatedVariantMongoReader;
import uk.ac.ebi.eva.accession.release.batch.io.merged_deprecated.EvaMergedDeprecatedVariantMongoReader;
import uk.ac.ebi.eva.accession.release.collectionNames.DbsnpCollectionNames;
import uk.ac.ebi.eva.accession.release.collectionNames.EvaCollectionNames;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_MERGED_DEPRECATED_VARIANT_READER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_MERGED_DEPRECATED_VARIANT_READER;

@Configuration
@Import({MongoConfiguration.class})
public class MergedDeprecatedVariantMongoReaderConfiguration {

    @Bean(DBSNP_MERGED_DEPRECATED_VARIANT_READER)
    @StepScope
    public ItemStreamReader<DbsnpClusteredVariantOperationEntity> mergedDeprecatedVariantMongoReaderDbsnp(
            InputParameters parameters, MongoClient mongoClient, MongoTemplate mongoTemplate,
            MongoProperties mongoProperties) {
        return new DbsnpMergedDeprecatedVariantMongoReader(parameters.getAssemblyAccession(), mongoClient,
                                                           mongoProperties.getDatabase(), mongoTemplate.getConverter(),
                                                           parameters.getChunkSize(), new DbsnpCollectionNames());
    }

    @Bean(EVA_MERGED_DEPRECATED_VARIANT_READER)
    @StepScope
    public ItemStreamReader<ClusteredVariantOperationEntity> mergedDeprecatedVariantMongoReaderEva(
            InputParameters parameters, MongoClient mongoClient, MongoTemplate mongoTemplate,
            MongoProperties mongoProperties) {
        return new EvaMergedDeprecatedVariantMongoReader(parameters.getAssemblyAccession(), mongoClient,
                                                         mongoProperties.getDatabase(), mongoTemplate.getConverter(),
                                                         parameters.getChunkSize(), new EvaCollectionNames());
    }
}
