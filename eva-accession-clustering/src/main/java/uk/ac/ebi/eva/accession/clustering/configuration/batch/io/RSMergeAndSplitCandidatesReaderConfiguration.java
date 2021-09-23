/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.clustering.configuration.batch.io;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.eva.accession.clustering.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;

import java.util.Collections;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_MERGE_CANDIDATES_READER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_SPLIT_CANDIDATES_READER;

@Configuration
@Import({MongoConfiguration.class, InputParametersConfiguration.class})
public class RSMergeAndSplitCandidatesReaderConfiguration {

    private static final String SUBMITTED_VARIANT_OPERATIONS_COLLECTION = "submittedVariantOperationEntity";

    private static final String ASSEMBLY_FIELD = "inactiveObjects.seq";

    private static final String SORT_FIELD = "accession";

    private static final String EVENT_TYPE_FIELD = "eventType";

    //TODO: Update this with the event type RS_SPLIT_CANDIDATES after that type is introduced in accession-commons
    public static final EventType SPLIT_CANDIDATES_EVENT_TYPE = EventType.RS_SPLIT_CANDIDATES;

    //TODO: Update this with the event type RS_MERGE_CANDIDATES after that type is introduced in accession-commons
    public static final EventType MERGE_CANDIDATES_EVENT_TYPE = EventType.RS_MERGE_CANDIDATES;

    public static Query getSplitCandidatesQuery(String assemblyAccession) {
        return Query.query(where(ASSEMBLY_FIELD).is(assemblyAccession))
                    .addCriteria(where(EVENT_TYPE_FIELD).is(SPLIT_CANDIDATES_EVENT_TYPE));
    }

    public static Query getMergeCandidatesQuery(String assemblyAccession) {
        return Query.query(where(ASSEMBLY_FIELD).is(assemblyAccession))
                    .addCriteria(where(EVENT_TYPE_FIELD).is(MERGE_CANDIDATES_EVENT_TYPE));
    }

    @Bean(RS_SPLIT_CANDIDATES_READER)
    public ItemReader<SubmittedVariantOperationEntity> rsSplitCandidatesReader(MongoTemplate mongoTemplate, InputParameters parameters) {
        MongoItemReader<SubmittedVariantOperationEntity> mongoItemReader = new MongoItemReader<>();
        mongoItemReader.setTemplate(mongoTemplate);
        mongoItemReader.setTargetType(SubmittedVariantOperationEntity.class);
        mongoItemReader.setCollection(SUBMITTED_VARIANT_OPERATIONS_COLLECTION);

        //See https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/#rangeid=1213746442
        Query query = getSplitCandidatesQuery(parameters.getAssemblyAccession());
        mongoItemReader.setQuery(query);
        mongoItemReader.setPageSize(parameters.getChunkSize());
        mongoItemReader.setSort(Collections.singletonMap(SORT_FIELD, Sort.Direction.ASC));
        return mongoItemReader;
    }

    @Bean(RS_MERGE_CANDIDATES_READER)
    public ItemReader<SubmittedVariantOperationEntity> rsMergeCandidatesReader(MongoTemplate mongoTemplate, InputParameters parameters) {
        MongoItemReader<SubmittedVariantOperationEntity> mongoItemReader = new MongoItemReader<>();
        mongoItemReader.setTemplate(mongoTemplate);
        mongoItemReader.setTargetType(SubmittedVariantOperationEntity.class);
        mongoItemReader.setCollection(SUBMITTED_VARIANT_OPERATIONS_COLLECTION);

        //See https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/#rangeid=1213746442
        Query query = getMergeCandidatesQuery(parameters.getAssemblyAccession());
        mongoItemReader.setQuery(query);
        mongoItemReader.setPageSize(parameters.getChunkSize());
        mongoItemReader.setSort(Collections.singletonMap(SORT_FIELD, Sort.Direction.ASC));
        return mongoItemReader;
    }
}
