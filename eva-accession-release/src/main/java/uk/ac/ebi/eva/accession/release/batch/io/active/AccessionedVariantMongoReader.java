/*
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
 */

package uk.ac.ebi.eva.accession.release.batch.io.active;

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
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.BooleanOperators;

import uk.ac.ebi.eva.accession.release.batch.io.VariantMongoAggregationReader;
import uk.ac.ebi.eva.accession.release.collectionNames.CollectionNames;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.models.VariantTypeToSOAccessionMap;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.orderBy;
import static org.springframework.data.mongodb.core.aggregation.ArrayOperators.Filter.filter;
import static org.springframework.data.mongodb.core.aggregation.ComparisonOperators.Eq.valueOf;
import static uk.ac.ebi.eva.accession.core.model.ISubmittedVariant.DEFAULT_ALLELES_MATCH;
import static uk.ac.ebi.eva.accession.core.model.ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH;
import static uk.ac.ebi.eva.accession.core.model.ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE;
import static uk.ac.ebi.eva.accession.core.model.ISubmittedVariant.DEFAULT_VALIDATED;

public class AccessionedVariantMongoReader extends VariantMongoAggregationReader {

    private static final Logger logger = LoggerFactory.getLogger(AccessionedVariantMongoReader.class);

    private static final List<String> allSubmittedVariantCollectionNames = Arrays.asList("submittedVariantEntity",
                                                                                         "dbsnpSubmittedVariantEntity");

    public AccessionedVariantMongoReader(String assemblyAccession, int taxonomyAccession,
                                         MongoClient mongoClient, String database, int chunkSize,
                                         CollectionNames names) {
        super(assemblyAccession, taxonomyAccession, mongoClient, database, chunkSize, names);
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        aggregate(names.getClusteredVariantEntity());
    }

    protected List<Bson> buildAggregation() {
        Bson match = Aggregates.match(Filters.eq(REFERENCE_ASSEMBLY_FIELD, assemblyAccession));
        Bson sort = Aggregates.sort(orderBy(ascending(CONTIG_FIELD, START_FIELD)));
        Bson singlemap = Aggregates.match(Filters.not(exists(MAPPING_WEIGHT_FIELD)));
        List<Bson> aggregation = new ArrayList<>(Arrays.asList(match, sort, singlemap));
        String tempArrayName = "ssArray";
        for (String submittedVariantCollectionName : allSubmittedVariantCollectionNames) {
            Bson lookup = Aggregates.lookup(submittedVariantCollectionName, ACCESSION_FIELD,
                                            CLUSTERED_VARIANT_ACCESSION_FIELD,
                                            submittedVariantCollectionName);
            aggregation.add(lookup);
        }
        // Concat ss entries from all submitted variant collections
        Bson concat = Aggregates.addFields(new Field<>(tempArrayName,
                                                       new Document("$concatArrays", allSubmittedVariantCollectionNames
                                                               .stream().map(v -> "$" + v)
                                                               .collect(Collectors.toList()))));
        // Filter out ss entries not belonging to the release assembly from ssArray field
        // created above from the lookup
        AggregationOperation addFieldsOperation =
                aggregationContext ->
                        new Document("$addFields",
                                     new Document(SS_INFO_FIELD,
                                                  filter(tempArrayName)
                                                          .as(SS_INFO_FIELD)
                                                          .by(BooleanOperators.And.and(
                                                                  valueOf(SS_INFO_FIELD + "." +
                                                                                  REFERENCE_ASSEMBLY_FIELD_IN_SUBMITTED_COLLECTIONS)
                                                                          .equalToValue(assemblyAccession),
                                                                  valueOf(SS_INFO_FIELD + "." +
                                                                                  TAXONOMY_FIELD)
                                                                          .equalToValue(taxonomyAccession)))
                                                          .toDocument(Aggregation.DEFAULT_CONTEXT)));
        Bson addSSInfoField = addFieldsOperation.toDocument(Aggregation.DEFAULT_CONTEXT);
        Map<String, Object> removalMap = allSubmittedVariantCollectionNames
                .stream().collect(Collectors.toMap(Function.identity(), v -> 0));
        removalMap.put(tempArrayName, 0);
        Bson removeTempArraysFromOutput = Aggregates.project(new Document(removalMap));
        // We only need the SS info field with the entries in the temp array filtered by the release assembly
        aggregation.addAll(Arrays.asList(concat, addSSInfoField, removeTempArraysFromOutput));
        logger.info("Issuing aggregation: {}", aggregation);
        return aggregation;
    }

    protected List<Variant> getVariants(Document clusteredVariant) {
        String contig = clusteredVariant.getString(CONTIG_FIELD);
        long start = clusteredVariant.getLong(START_FIELD);
        long rs = clusteredVariant.getLong(ACCESSION_FIELD);
        String type = clusteredVariant.getString(TYPE_FIELD);
        String sequenceOntology = VariantTypeToSOAccessionMap.getSequenceOntologyAccession(VariantType.valueOf(type));
        boolean validated = clusteredVariant.getBoolean(VALIDATED_FIELD, DEFAULT_VALIDATED);

        Map<String, Variant> variants = new HashMap<>();
        Collection<Document> submittedVariants = (Collection<Document>)clusteredVariant.get(SS_INFO_FIELD);
        boolean remappedRS = submittedVariants.stream()
                                              .allMatch(sve -> Objects.nonNull(sve.getString("remappedFrom")));

        for (Document submittedVariant : submittedVariants) {
            long submittedVariantStart = submittedVariant.getLong(START_FIELD);
            String submittedVariantContig = submittedVariant.getString(CONTIG_FIELD);
            if (!isSameLocation(contig, start, submittedVariantContig, submittedVariantStart, type)) {
                continue;
            }
            String reference = submittedVariant.getString(REFERENCE_ALLELE_FIELD);
            String alternate = submittedVariant.getString(ALTERNATE_ALLELE_FIELD);
            String study = submittedVariant.getString(STUDY_FIELD);
            boolean submittedVariantValidated = submittedVariant.getBoolean(VALIDATED_FIELD, DEFAULT_VALIDATED);
            boolean allelesMatch = submittedVariant.getBoolean(ALLELES_MATCH_FIELD, DEFAULT_ALLELES_MATCH);
            boolean assemblyMatch = submittedVariant.getBoolean(ASSEMBLY_MATCH_FIELD, DEFAULT_ASSEMBLY_MATCH);
            boolean evidence = submittedVariant.getBoolean(SUPPORTED_BY_EVIDENCE_FIELD, DEFAULT_SUPPORTED_BY_EVIDENCE);

            VariantSourceEntry sourceEntry = buildVariantSourceEntry(study, sequenceOntology, validated,
                                                                     submittedVariantValidated, allelesMatch,
                                                                     assemblyMatch, evidence, remappedRS);

            addToVariants(variants, contig, submittedVariantStart, rs, reference, alternate, sourceEntry);
        }
        return new ArrayList<>(variants.values());
    }

    /**
     * The query performed in mongo can retrieve more variants than the actual ones because in some cases the same
     * clustered variant is mapped against multiple locations. So we need to check that that clustered variant we are
     * processing only appears in the VCF release file with the alleles from submitted variants matching the location.
     */
    private boolean isSameLocation(String contig, long start, String submittedVariantContig, long submittedVariantStart,
                                   String type) {
        return contig.equals(submittedVariantContig) && isSameStart(start, submittedVariantStart, type);
    }

    /**
     * The start is considered to be the same when:
     * - start in clustered and submitted variant match
     * - start in clustered and submitted variant have a difference of 1
     *
     * The start position can be different in ambiguous INDELS because the renormalization is only applied to
     * submitted variants. In those cases the start in the clustered and submitted variants will not exactly match but
     * the difference should be 1
     *
     * Example:
     * RS (assembly: GCA_000309985.1, accession: 268233057, chromosome: CM001642.1, start: 7356605, type: INS)
     * SS (assembly: GCA_000309985.1, accession: 490570267, chromosome: CM001642.1, start: 7356604, reference: ,
     *     alternate: AGAGCTATGATCTTCGGAAGGAGAAGGAGAAGGAAAAGATTCATGACGTCCAC)
     */
    private boolean isSameStart(long clusteredVariantStart, long submittedVariantStart, String type) {
         return clusteredVariantStart == submittedVariantStart
                 || (isIndel(type) && Math.abs(clusteredVariantStart - submittedVariantStart) == 1L);
    }

    private boolean isIndel(String type) {
        return type.equals(VariantType.INS.toString()) || type.equals(VariantType.DEL.toString());
    }
}
