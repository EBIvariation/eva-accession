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
package uk.ac.ebi.eva.accession.release.batch.io.deprecated;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Filters;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;

import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.eva.accession.release.batch.io.VariantMongoAggregationReader;
import uk.ac.ebi.eva.accession.release.collectionNames.CollectionNames;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.orderBy;

public class DeprecatedVariantMongoReader extends VariantMongoAggregationReader {

    private static final Logger logger = LoggerFactory.getLogger(DeprecatedVariantMongoReader.class);

    private static final String EVENT_TYPE_FIELD = "eventType";

    private static final String INACTIVE_OBJECTS = "inactiveObjects";

    private static final List<String> allSubmittedVariantOperationCollectionNames = Arrays.asList(
            "submittedVariantOperationEntity",
            "dbsnpSubmittedVariantOperationEntity"
    );

    public DeprecatedVariantMongoReader(String assemblyAccession, int taxonomyAccession, MongoClient mongoClient,
                                        String database, int chunkSize, CollectionNames names) {
        super(assemblyAccession, taxonomyAccession, mongoClient, database, chunkSize, names);
    }

    private String getInactiveField(String field) {
        return INACTIVE_OBJECTS + "." + field;
    }

    @Override
    protected List<Bson> buildAggregation() {
        Bson matchAssembly = Aggregates.match(Filters.eq(getInactiveField(REFERENCE_ASSEMBLY_FIELD),
                                                         assemblyAccession));
        Bson matchMerged = Aggregates.match(Filters.eq(EVENT_TYPE_FIELD, EventType.DEPRECATED.toString()));
        Bson sort = Aggregates.sort(orderBy(ascending(getInactiveField(CONTIG_FIELD), getInactiveField(START_FIELD))));
        List<Bson> aggregation = new ArrayList<>(Arrays.asList(matchAssembly, matchMerged, sort));

        for (String submittedVariantOperationCollectionName : allSubmittedVariantOperationCollectionNames) {
            Bson lookup = Aggregates.lookup(submittedVariantOperationCollectionName, ACCESSION_FIELD,
                                                          getInactiveField(CLUSTERED_VARIANT_ACCESSION_FIELD),
                                                          submittedVariantOperationCollectionName);
            aggregation.add(lookup);
        }
        // Concat entries from all submitted variant operation collections
        Bson ssConcat = Aggregates.addFields(new Field<>(SS_INFO_FIELD,
                                                         new Document("$concatArrays", allSubmittedVariantOperationCollectionNames
                                                                 .stream().map(v -> "$" + v)
                                                                 .collect(Collectors.toList()))));
        aggregation.add(ssConcat);
        // Ensure that we are only retrieving the variants with the relevant taxonomy
        // and event type in the Submitted operations collections
        Bson matchTaxonomyAndEventType = Aggregates.match(Filters.and(
                Filters.ne(SS_INFO_FIELD, Collections.emptyList()),
                Filters.eq(SS_INFO_FIELD + "." +
                                   getInactiveField(REFERENCE_ASSEMBLY_FIELD_IN_SUBMITTED_COLLECTIONS),
                           this.assemblyAccession),
                Filters.eq(SS_INFO_FIELD + "." + getInactiveField(TAXONOMY_FIELD), this.taxonomyAccession),
                Filters.in(SS_INFO_FIELD + "." + EVENT_TYPE_FIELD,
                           Arrays.asList(EventType.UPDATED.toString(), EventType.DEPRECATED.toString()))));
        aggregation.add(matchTaxonomyAndEventType);
        logger.info("Issuing aggregation: {}", aggregation);
        logger.info(aggregation.toString());
        return aggregation;
    }

    @Override
    protected List<Variant> getVariants(Document deprecatedVariant) {
        Collection<Document> inactiveObjects = (Collection<Document>) deprecatedVariant.get(INACTIVE_OBJECTS);
        Collection<Document> submittedVariantOperations = (Collection<Document>) deprecatedVariant.get(SS_INFO_FIELD);
        Document inactiveEntity = inactiveObjects.iterator().next();
        String contig = inactiveEntity.getString(VariantMongoAggregationReader.CONTIG_FIELD);
        long start = inactiveEntity.getLong(VariantMongoAggregationReader.START_FIELD);
        String type = inactiveEntity.getString(TYPE_FIELD);

        for (Document submittedVariantOperation : submittedVariantOperations) {
            Collection<Document> inactiveEntitySubmittedVariant = (Collection<Document>) submittedVariantOperation
                    .get("inactiveObjects");
            Document submittedVariant = inactiveEntitySubmittedVariant.iterator().next();
            long submittedVariantStart = submittedVariant.getLong(START_FIELD);
            String submittedVariantContig = submittedVariant.getString(CONTIG_FIELD);
            String reference = submittedVariant.getString("ref");
            String alternate = submittedVariant.getString("alt");

            if (isSameLocation(contig, start, submittedVariantContig, submittedVariantStart, type)) {
                // Since we only need evidence that at least one submitted variant agrees
                // with the deprecated RS in locus, we just return one variant record per RS
                Variant variantToReturn = new Variant(contig, start,
                                                      start + Math.max(reference.length(), alternate.length()) - 1,
                                                      reference, alternate);
                variantToReturn.setMainId("rs" + deprecatedVariant.getLong("accession"));
                return Arrays.asList(variantToReturn);
            }
        }

        return new ArrayList<>();
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        aggregate(names.getClusteredVariantOperationEntity());
    }
}
