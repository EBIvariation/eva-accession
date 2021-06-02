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
package uk.ac.ebi.eva.remapping.source.batch.io;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import uk.ac.ebi.eva.accession.core.batch.io.MongoDbCursorItemReader;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.util.List;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class EvaSubmittedVariantMongoReader extends MongoDbCursorItemReader<SubmittedVariantEntity> {

    public static final String REFERENCE_SEQUENCE_FIELD = "seq";

    public static final String PROJECT_KEY = "study";

    public static final String TAXONOMY_KEY = "tax";

    public EvaSubmittedVariantMongoReader(String assemblyAccession, MongoTemplate mongoTemplate,
                                          List<String> projects, int taxonomy) {
        setTemplate(mongoTemplate);
        setTargetType(SubmittedVariantEntity.class);

        Criteria criteria = where(REFERENCE_SEQUENCE_FIELD).is(assemblyAccession);

        if (!CollectionUtils.isEmpty(projects)) {
            criteria.and(PROJECT_KEY).in(projects);
        }

        if (taxonomy != 0) {
            criteria.and(TAXONOMY_KEY).is(taxonomy);
        }

        setQuery(new Query(criteria));
    }

}
