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

package uk.ac.ebi.eva.accession.release.batch.io.merged;

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

import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.release.batch.io.VariantMongoAggregationReader;
import uk.ac.ebi.eva.accession.release.collectionNames.CollectionNames;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.models.VariantTypeToSOAccessionMap;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.orderBy;
import static uk.ac.ebi.eva.accession.core.model.ISubmittedVariant.DEFAULT_ALLELES_MATCH;
import static uk.ac.ebi.eva.accession.core.model.ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH;
import static uk.ac.ebi.eva.accession.core.model.ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE;
import static uk.ac.ebi.eva.accession.core.model.ISubmittedVariant.DEFAULT_VALIDATED;

public class MergedVariantMongoReader extends VariantMongoAggregationReader {

    private static final Logger logger = LoggerFactory.getLogger(MergedVariantMongoReader.class);

    private static final String INACTIVE_OBJECTS = "inactiveObjects";

    private static final String MERGE_INTO_FIELD = "mergeInto";

    private static final String EVENT_TYPE_FIELD = "eventType";

    private static final String REASON = "reason";

    private static final String MERGE_OPERATION_REASON_FIRST_WORD = "Original";

    private static final String DECLUSTER_OPERATION_REASON_FIRST_WORD = "Declustered: ";

    private static final String ACTIVE_RS = "activeRs";

    private static final List<String> allSubmittedVariantOperationCollectionNames = Arrays.asList(
            "submittedVariantOperationEntity",
            "dbsnpSubmittedVariantOperationEntity"
    );

    private static final List<String> allClusteredVariantCollectionNames = Arrays.asList(
            "clusteredVariantEntity",
            "dbsnpClusteredVariantEntity"
    );

    public MergedVariantMongoReader(String assemblyAccession, int taxonomyAccession, MongoClient mongoClient,
                                    String database, int chunkSize, CollectionNames names) {
        super(assemblyAccession, taxonomyAccession, mongoClient, database, chunkSize, names);
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        aggregate(names.getClusteredVariantOperationEntity());
    }

    @Override
    protected List<Bson> buildAggregation() {
        Bson matchAssembly = Aggregates.match(Filters.eq(getInactiveField(REFERENCE_ASSEMBLY_FIELD),
                                                         assemblyAccession));
        Bson matchMerged = Aggregates.match(Filters.eq(EVENT_TYPE_FIELD, EventType.MERGED.toString()));
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
                Filters.eq(SS_INFO_FIELD + "." + EVENT_TYPE_FIELD,EventType.UPDATED.toString())));
        aggregation.add(matchTaxonomyAndEventType);

        // Similarly look in both clustered variant collections for active RS
        for (String clusteredVariantCollectionName : allClusteredVariantCollectionNames) {
            Bson lookupClusteredVariants = Aggregates.lookup(clusteredVariantCollectionName, MERGE_INTO_FIELD,
                                                             ACCESSION_FIELD, clusteredVariantCollectionName);
            aggregation.add(lookupClusteredVariants);
        }
        Bson rsConcat = Aggregates.addFields(new Field<>(ACTIVE_RS,
                                                         new Document("$concatArrays", allClusteredVariantCollectionNames
                                                                 .stream().map(v -> "$" + v)
                                                                 .collect(Collectors.toList()))));
        aggregation.add(rsConcat);
        Bson matchOnlyNonEmptyActiveRS = Aggregates.match(Filters.ne(ACTIVE_RS, Collections.emptyList()));
        aggregation.add(matchOnlyNonEmptyActiveRS);
        logger.info("Issuing aggregation: {}", aggregation);
        return aggregation;
    }

    private String getInactiveField(String field) {
        return INACTIVE_OBJECTS + "." + field;
    }

    @Override
    protected List<Variant> getVariants(Document mergedVariant) {
        Collection<Document> inactiveObjects = (Collection<Document>) mergedVariant.get(INACTIVE_OBJECTS);
        if (inactiveObjects.size() > 1) {
            throw new AssertionError("The class '" + this.getClass().getSimpleName()
                                     + "' was designed assuming there's only one element in the field "
                                     + "'" + INACTIVE_OBJECTS + "'. Found " + inactiveObjects.size()
                                     + " elements in _id=" + mergedVariant.get(ACCESSION_FIELD));
        }
        Document inactiveEntity = inactiveObjects.iterator().next();
        String contig = inactiveEntity.getString(VariantMongoAggregationReader.CONTIG_FIELD);
        long start = inactiveEntity.getLong(VariantMongoAggregationReader.START_FIELD);
        long rs = mergedVariant.getLong(ACCESSION_FIELD);
        long mergedInto = mergedVariant.getLong(MERGE_INTO_FIELD);
        String type = inactiveEntity.getString(TYPE_FIELD);
        String sequenceOntology = VariantTypeToSOAccessionMap.getSequenceOntologyAccession(VariantType.valueOf(type));
        boolean validated = inactiveEntity.getBoolean(VALIDATED_FIELD, IClusteredVariant.DEFAULT_VALIDATED);

        Map<String, Variant> mergedVariants = new HashMap<>();
        Collection<Document> submittedVariantOperations = (Collection<Document>) mergedVariant.get(SS_INFO_FIELD);
        boolean remappedRS = submittedVariantOperations.stream()
                                                       .map(e -> (Collection<Document>) e.get("inactiveObjects"))
                                                       .flatMap(Collection::stream)
                                                       .allMatch(sve -> Objects.nonNull(sve.getString("remappedFrom")));

        Collection<Document> activeClusteredVariant = (Collection<Document>) mergedVariant.get(ACTIVE_RS);
        if (activeClusteredVariant.isEmpty()) {
            return Collections.emptyList();
        }

        boolean hasSubmittedVariantsDeclustered = false;
        for (Document submittedVariantOperation : submittedVariantOperations) {
            if (submittedVariantOperation.getString(EVENT_TYPE_FIELD).equals(EventType.UPDATED.toString())
                    && submittedVariantOperation.getString(REASON).startsWith(MERGE_OPERATION_REASON_FIRST_WORD)) {
                Collection<Document> inactiveEntitySubmittedVariant = (Collection<Document>) submittedVariantOperation
                        .get("inactiveObjects");
                Document submittedVariant = inactiveEntitySubmittedVariant.iterator().next();
                long submittedVariantStart = submittedVariant.getLong(START_FIELD);
                String submittedVariantContig = submittedVariant.getString(CONTIG_FIELD);

                if (!isSameLocation(contig, start, submittedVariantContig, submittedVariantStart, type)){
                    continue;
                }

                String reference = submittedVariant.getString(REFERENCE_ALLELE_FIELD);
                String alternate = submittedVariant.getString(ALTERNATE_ALLELE_FIELD);
                String study = submittedVariant.getString(STUDY_FIELD);
                boolean submittedVariantValidated = submittedVariant.getBoolean(VALIDATED_FIELD, DEFAULT_VALIDATED);
                boolean allelesMatch = submittedVariant.getBoolean(ALLELES_MATCH_FIELD, DEFAULT_ALLELES_MATCH);
                boolean assemblyMatch = submittedVariant.getBoolean(ASSEMBLY_MATCH_FIELD, DEFAULT_ASSEMBLY_MATCH);
                boolean evidence = submittedVariant
                        .getBoolean(SUPPORTED_BY_EVIDENCE_FIELD, DEFAULT_SUPPORTED_BY_EVIDENCE);

                VariantSourceEntry sourceEntry = buildVariantSourceEntry(study, sequenceOntology, validated,
                                                                         submittedVariantValidated, allelesMatch,
                                                                         assemblyMatch, evidence, remappedRS,
                                                                         mergedInto);

                addToVariants(mergedVariants, contig, submittedVariantStart, rs, reference, alternate, sourceEntry);
            }

            if (submittedVariantOperation.getString(EVENT_TYPE_FIELD).equals(EventType.UPDATED.toString())
                    && submittedVariantOperation.getString(REASON).startsWith(DECLUSTER_OPERATION_REASON_FIRST_WORD)) {
                hasSubmittedVariantsDeclustered = true;
            }
        }

        if (!hasSubmittedVariantsDeclustered && mergedVariants.isEmpty()) {
            logger.warn("Found merge operation for rs" + rs + " but no SS IDs updates " +
                                "(merge/update) in the collection containing operations. " +
                                "This could have possibly happened on a remapped variant " +
                                "because there was a subsequent split issued for this RS due to loci disagreement " +
                                "between the RS and the SS.");
            return Collections.emptyList();
        }

        return new ArrayList<>(mergedVariants.values());
    }
}
