/*
 *
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
 *
 */
package uk.ac.ebi.eva.accession.core.persistence;

import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import uk.ac.ebi.ampt2d.commons.accession.persistence.models.AccessionProjection;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.repository.BasicMongoDbAccessionedCustomRepositoryImpl;

import java.util.List;
import java.util.stream.Collectors;

public class SubmittedVariantAccessioningRepositoryImpl
        extends BasicMongoDbAccessionedCustomRepositoryImpl<Long, SubmittedVariantEntity> {

    private MongoOperations mongoOperations;

    public SubmittedVariantAccessioningRepositoryImpl(MongoTemplate mongoTemplate) {
        super(SubmittedVariantEntity.class, mongoTemplate);
        mongoOperations = mongoTemplate;
    }

    List<AccessionProjection<Long>> findByAccessionGreaterThanEqualAndAccessionLessThanEqual(Long start, Long end) {
        return mongoOperations.find(Query.query(Criteria.where("accession").gte(start).lt(end)),
                                    SubmittedVariantEntity.class)
                              .stream()
                              .map(AccessionedDocument::getAccession)
                              .map(accession -> (AccessionProjection<Long>) () -> accession)
                              .collect(Collectors.toList());
    }
}
