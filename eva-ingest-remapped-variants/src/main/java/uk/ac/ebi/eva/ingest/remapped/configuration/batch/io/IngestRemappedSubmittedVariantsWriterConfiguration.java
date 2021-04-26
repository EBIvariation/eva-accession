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
package uk.ac.ebi.eva.ingest.remapped.configuration.batch.io;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.ingest.remapped.batch.io.RemappedSubmittedVariantsWriter;

import static uk.ac.ebi.eva.ingest.remapped.configuration.BeanNames.REMAPPED_SUBMITTED_VARIANTS_WRITER;

@Configuration
@Import({SubmittedVariantAccessioningConfiguration.class, MongoConfiguration.class})
public class IngestRemappedSubmittedVariantsWriterConfiguration {

    @Bean(REMAPPED_SUBMITTED_VARIANTS_WRITER)
    public RemappedSubmittedVariantsWriter remappedSubmittedVariantsWriter(MongoTemplate mongoTemplate){
        return new RemappedSubmittedVariantsWriter(mongoTemplate);
    }
}
