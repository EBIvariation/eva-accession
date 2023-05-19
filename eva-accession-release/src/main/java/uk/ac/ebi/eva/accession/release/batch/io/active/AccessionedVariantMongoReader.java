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
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.orderBy;

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
        Bson match = Aggregates.match(eq(REFERENCE_ASSEMBLY_FIELD, assemblyAccession));
        Bson sort = Aggregates.sort(orderBy(ascending(CONTIG_FIELD, START_FIELD)));
        Bson singlemap = Aggregates.match(Filters.not(exists(MAPPING_WEIGHT_FIELD)));
        List<Bson> aggregation = new ArrayList<>(Arrays.asList(match, sort, singlemap));

        for (String submittedVariantCollectionName : allSubmittedVariantCollectionNames) {
            String lookupQuery = "{ $lookup: { " +
                    String.format("from: \"%s\",", submittedVariantCollectionName) +
                    String.format("let: { rsAccession: \"$%s\" },", ACCESSION_FIELD) +
                    "pipeline: [{" +
                    "$match: {$expr: {$and: [" +
                    String.format("{ $eq: ['$%s', \"$$rsAccession\"]},",
                                  CLUSTERED_VARIANT_ACCESSION_FIELD) +
                    String.format("{ $eq: [\"$%s\", \"%s\"]},",
                                  REFERENCE_ASSEMBLY_FIELD_IN_SUBMITTED_COLLECTIONS,
                                  this.assemblyAccession) +
                    String.format("{ $eq: [\"$%s\", %d]}]}}}],",
                                  TAXONOMY_FIELD, this.taxonomyAccession) +
                    String.format("as: \"%s\"}}", submittedVariantCollectionName);
            logger.info(lookupQuery);
            Bson lookup = Aggregation.DEFAULT_CONTEXT.getMappedObject(Document.parse(lookupQuery));
            aggregation.add(lookup);
        }
        // Concat ss entries from all submitted variant collections
        Bson concat = Aggregates.addFields(new Field<>(SS_INFO_FIELD,
                                                       new Document("$concatArrays", allSubmittedVariantCollectionNames
                                                               .stream().map(v -> "$" + v)
                                                               .collect(Collectors.toList()))));
        // We only need the SS info field
        aggregation.add(concat);
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
}
