/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.clustering.batch.io.qc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class MissingCveReporter implements ItemWriter<RSHashPair> {

    private static final Logger logger = LoggerFactory.getLogger(MissingCveReporter.class);

    private static final String ID_FIELD = "_id";

    private final MongoTemplate mongoTemplate;

    public MissingCveReporter(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void write(@Nonnull List<? extends RSHashPair> rsHashPairs) {
        Map<String, Long> hashToRs = rsHashPairs.stream()
                                                .collect(Collectors.toMap(RSHashPair::getHash, RSHashPair::getRsId));
        Map<String, ClusteredVariantEntity> results = findClusteredVariantsInDb(hashToRs);

        for (String hash : hashToRs.keySet()) {
            if (!results.containsKey(hash)) {
                logger.error("Could not find clustered variant with hash {}", hash);
                continue;
            }
            Long dbRs = results.get(hash).getAccession();
            Long expectedRs = hashToRs.get(hash);
            if (!Objects.equals(dbRs, expectedRs)) {
                logger.error("Hash {} has rs{} in db, expected rs{}", hash, dbRs, expectedRs);
            }
        }
    }

    private Map<String, ClusteredVariantEntity> findClusteredVariantsInDb(Map<String, Long> hashToRs) {
        Query query = query(where(ID_FIELD).in(hashToRs.keySet()));
        List<ClusteredVariantEntity> evaResults = mongoTemplate.find(query, ClusteredVariantEntity.class);
        List<DbsnpClusteredVariantEntity> dbsnpResults = mongoTemplate.find(query, DbsnpClusteredVariantEntity.class);
        return Stream.concat(evaResults.stream(), dbsnpResults.stream())
                     .collect(Collectors.toMap(ClusteredVariantEntity::getHashedMessage, Function.identity()));
    }

}
