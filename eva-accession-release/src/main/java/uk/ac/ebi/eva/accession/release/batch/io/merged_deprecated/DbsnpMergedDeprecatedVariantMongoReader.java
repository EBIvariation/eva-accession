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

package uk.ac.ebi.eva.accession.release.batch.io.merged_deprecated;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import org.bson.Document;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.release.collectionNames.CollectionNames;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Reads historical variants, that have been merged into a later deprecate one, from a MongoDB database.
 */
public class DbsnpMergedDeprecatedVariantMongoReader
        extends MergedDeprecatedVariantMongoReader<DbsnpClusteredVariantOperationEntity> {

    private MongoConverter mongoConverter;

    public DbsnpMergedDeprecatedVariantMongoReader(String assemblyAccession, int taxonomyAccession,
                                                   MongoClient mongoClient, String database,
                                                   MongoConverter mongoConverter, int chunkSize,
                                                   CollectionNames names) {
        super(assemblyAccession, taxonomyAccession, mongoClient, database, chunkSize, names);
        this.mongoConverter = mongoConverter;
    }

    /**
     * Converts to {@link DbsnpClusteredVariantOperationEntity} using MongoConverter.
     * <p>
     * Note how we also use the MongoConverter to convert the internal "inactiveObjects". If we didn't do that, the
     * converter puts in
     * {@link uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument#inactiveObjects} a list
     * of Documents instead of a list of DbsnpClusteredVariantInactiveEntity.
     */
    protected DbsnpClusteredVariantOperationEntity getEntity(Document operation) {
        List<Document> objects = (List<Document>) operation.get(INACTIVE_OBJECTS);
        operation.put(INACTIVE_OBJECTS, objects.stream()
                                               .map(BasicDBObject::new)
                                               .map(o -> mongoConverter.read(DbsnpClusteredVariantInactiveEntity.class,
                                                                             o))
                                               .collect(Collectors.toList()));

        return mongoConverter.read(DbsnpClusteredVariantOperationEntity.class, new BasicDBObject(operation));
    }
}
