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
package uk.ac.ebi.eva.accession.clustering.batch.io.qc;

import com.mongodb.MongoBulkWriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;

import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringWriter;
import uk.ac.ebi.eva.accession.clustering.batch.io.qc.QCMongoCollections.qcRSHashInSS;
import uk.ac.ebi.eva.accession.clustering.batch.io.qc.QCMongoCollections.qcRSIdInSS;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static uk.ac.ebi.eva.accession.core.exceptions.MongoBulkWriteExceptionUtils.extractUniqueHashesForDuplicateKeyError;

public class PendingMergeSplitReporter implements ItemWriter<SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(PendingMergeSplitReporter.class);

    private static final String IDAttribute = "_id";

    private final String assemblyAccession;

    private final ClusteringWriter clusteringWriter;

    private final MongoTemplate mongoTemplate;

    public PendingMergeSplitReporter(String assemblyAccession, ClusteringWriter clusteringWriter,
                                     MongoTemplate mongoTemplate) {
        this.assemblyAccession = assemblyAccession;
        this.clusteringWriter = clusteringWriter;
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void write(@Nonnull List<? extends SubmittedVariantEntity> submittedVariantEntities)
            throws MongoBulkWriteException, AccessionCouldNotBeGeneratedException {
        if (submittedVariantEntities.size() > 0) {
            reportPendingSplitsAndMerges(submittedVariantEntities);
        }
    }

    private void reportPendingSplitsAndMerges(List<? extends SubmittedVariantEntity> submittedVariantEntities) {
        Map<String, Long> hashAndAssociatedRS = new HashMap<>();
        Map<String, String> rsAndAssociatedHash = new HashMap<>();
        String assemblyAccessionPrefix = QCMongoCollections.getAssemblyAccessionPrefix(this.assemblyAccession);
        for (SubmittedVariantEntity submittedVariantEntity :submittedVariantEntities) {
            Long rsID = submittedVariantEntity.getClusteredVariantAccession();
            if (Objects.nonNull(rsID)) {
                String rsHash = clusteringWriter.toClusteredVariantEntity(submittedVariantEntity).getHashedMessage();
                reportMultipleRSWithSameHash(hashAndAssociatedRS, assemblyAccessionPrefix, rsID, rsHash);
                hashAndAssociatedRS.put(assemblyAccessionPrefix + rsHash, rsID);
                reportSameRSWithMultipleHashes(rsAndAssociatedHash, assemblyAccessionPrefix, rsID, rsHash);
                rsAndAssociatedHash.put(assemblyAccessionPrefix + rsID, rsHash);
            }
        }

        BulkOperations bulkHashInsert = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                qcRSHashInSS.class);
        hashAndAssociatedRS.forEach((key, value) -> bulkHashInsert.insert(new qcRSHashInSS(key, value)));
        BulkOperations bulkIDInsert = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                qcRSIdInSS.class);
        rsAndAssociatedHash.forEach((key, value) -> bulkIDInsert.insert(new qcRSIdInSS(key, value)));

        try {
            if (hashAndAssociatedRS.size() > 1) {
                bulkHashInsert.execute();
            }
        }
        catch (DuplicateKeyException duplicateKeyException) {
            MongoBulkWriteException writeException = ((MongoBulkWriteException) duplicateKeyException.getCause());
            for (String hashWithAssemblyPrefix : extractUniqueHashesForDuplicateKeyError(writeException).collect(
                    Collectors.toList())) {
                qcRSHashInSS result = this.mongoTemplate.findOne(query(where(IDAttribute).is(hashWithAssemblyPrefix)),
                                                                 qcRSHashInSS.class);
                reportMultipleRSWithSameHash (hashAndAssociatedRS, assemblyAccessionPrefix, result.getRsID(),
                                              hashWithAssemblyPrefix.replace(assemblyAccessionPrefix, ""));
            };
        }

        try {
            if (rsAndAssociatedHash.size() > 1) {
                bulkIDInsert.execute();
            }
        }
        catch (DuplicateKeyException duplicateKeyException) {
            MongoBulkWriteException writeException = ((MongoBulkWriteException) duplicateKeyException.getCause());
            for (String idWithAssemblyPrefix : extractUniqueHashesForDuplicateKeyError(writeException).collect(
                    Collectors.toList())) {
                qcRSIdInSS result = this.mongoTemplate.findOne(query(where(IDAttribute).is(idWithAssemblyPrefix)),
                                                               qcRSIdInSS.class);
                reportSameRSWithMultipleHashes(rsAndAssociatedHash, assemblyAccessionPrefix,
                                               Long.parseLong(idWithAssemblyPrefix.replace(assemblyAccessionPrefix, "")),
                                               result.getHash());
            };
        }
    }

    private void reportSameRSWithMultipleHashes(Map<String, String> rsAndAssociatedHash, String assemblyAccessionPrefix,
                                                Long rsID, String rsHash) {
        String previousHashWithTheSameRSID = rsAndAssociatedHash.get(assemblyAccessionPrefix + rsID);
        if (Objects.nonNull(previousHashWithTheSameRSID) && !Objects.equals(previousHashWithTheSameRSID, rsHash)) {
            logger.error("Same RS ID rs{} has multiple hashes {} and {}",
                    rsID, previousHashWithTheSameRSID, rsHash);
        }
    }

    private void reportMultipleRSWithSameHash(Map<String, Long> hashAndAssociatedRS, String assemblyAccessionPrefix,
                                              Long rsID, String rsHash) {
        Long previousRSIDWithTheSameHash = hashAndAssociatedRS.get(assemblyAccessionPrefix + rsHash);
        if (Objects.nonNull(previousRSIDWithTheSameHash) && !Objects.equals(previousRSIDWithTheSameHash, rsID)) {
            logger.error("Multiple RS IDs rs{} and rs{} have the same hash {}",
                    previousRSIDWithTheSameHash, rsID, rsHash);
        }
    }
}
