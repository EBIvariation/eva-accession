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

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import uk.ac.ebi.ampt2d.commons.accession.persistence.IAccessionedObjectCustomRepository;

import java.util.Collection;
import java.util.Set;

public class SubmittedVariantAccessioningRepositoryImpl implements IAccessionedObjectCustomRepository {

    private static final String ID = "_id";

    private static final String ACTIVE = "active";

    private MongoTemplate mongoTemplate;

    public SubmittedVariantAccessioningRepositoryImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void enableByHashedMessageIn(Set<String> set) {
        mongoTemplate.updateMulti(new Query(Criteria.where(ID).in(set)), Update.update(ACTIVE, true),
                                  SubmittedVariantEntity.class);
    }

    @Override
    public <ENTITY> void insert(Collection<ENTITY> collection) {
        mongoTemplate.insert(collection, SubmittedVariantEntity.class);
    }
}
