/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.remapping.ingest.configuration.batch.io;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.remapping.ingest.batch.io.RemappedSubmittedVariantsWriter;
import uk.ac.ebi.eva.remapping.ingest.batch.listeners.RemappingIngestCounts;
import uk.ac.ebi.eva.remapping.ingest.configuration.BeanNames;
import uk.ac.ebi.eva.remapping.ingest.parameters.InputParameters;

@Configuration
@Import({MongoConfiguration.class})
public class IngestRemappedSubmittedVariantsWriterConfiguration {

    @Bean(BeanNames.REMAPPED_SUBMITTED_VARIANTS_WRITER)
    public RemappedSubmittedVariantsWriter remappedSubmittedVariantsWriter(MongoTemplate mongoTemplate,
                                                                           InputParameters inputParameters,
                                                                           RemappingIngestCounts remappingIngestCounts){
        return new RemappedSubmittedVariantsWriter(mongoTemplate, getCollection(inputParameters.getLoadTo()),
                                                   remappingIngestCounts);
    }

    private String getCollection(String loadTo) {
        if (loadTo == null) {
            throw new IllegalArgumentException("Please provide the target collection (EVA or DBSNP)");
        }else if (loadTo.toUpperCase().equals("EVA")) {
            return BeanNames.SUBMITTED_VARIANT_ENTITY;
        } else if (loadTo.toUpperCase().equals("DBSNP")) {
            return BeanNames.DBSNP_SUBMITTED_VARIANT_ENTITY;
        }
        throw new IllegalArgumentException("loadTo parameter should be either EVA or DBSNP");
    }
}
