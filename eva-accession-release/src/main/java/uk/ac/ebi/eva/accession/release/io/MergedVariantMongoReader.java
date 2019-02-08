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

package uk.ac.ebi.eva.accession.release.io;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;

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

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.orderBy;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ALLELES_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_VALIDATED;

public class MergedVariantMongoReader extends VariantMongoAggregationReader {

    private static final Logger logger = LoggerFactory.getLogger(MergedVariantMongoReader.class);

    private static final String DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY = "dbsnpClusteredVariantOperationEntity";

    private static final String INACTIVE_OBJECTS = "inactiveObjects";

    private static final String REFERENCE_ASSEMBLY_FIELD = INACTIVE_OBJECTS + ".asm";

    private static final String CONTIG_FIELD = INACTIVE_OBJECTS + "." + VariantMongoAggregationReader.CONTIG_FIELD;

    private static final String START_FIELD = INACTIVE_OBJECTS + "." + VariantMongoAggregationReader.START_FIELD;

    private static final String TYPE_KEY = "type";

    private static final String MERGE_INTO_FIELD = "mergeInto";

    public MergedVariantMongoReader(String assemblyAccession, MongoClient mongoClient, String database) {
        super(assemblyAccession, mongoClient, database);
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        aggregate(DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY);
    }

    @Override
    List<Bson> buildAggregation() {
        Bson match = Aggregates.match(Filters.eq(REFERENCE_ASSEMBLY_FIELD, assemblyAccession));
        Bson lookup = Aggregates.lookup(DBSNP_SUBMITTED_VARIANT_ENTITY, MERGE_INTO_FIELD,
                                        CLUSTERED_VARIANT_ACCESSION_FIELD, SS_INFO_FIELD);
        Bson sort = Aggregates.sort(orderBy(ascending(CONTIG_FIELD, START_FIELD)));
        List<Bson> aggregation = Arrays.asList(match, lookup, sort);
        logger.info("Issuing aggregation: {}", aggregation);
        return aggregation;
    }

    List<Variant> getVariants(Document mergedVariant) {
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
        VariantType type = VariantType.valueOf(inactiveEntity.getString(TYPE_KEY));
        String sequenceOntology = VariantTypeToSOAccessionMap.getSequenceOntologyAccession(type);
        boolean validated = inactiveEntity.getBoolean(VALIDATED_FIELD);

        Map<String, Variant> variants = new HashMap<>();
        Collection<Document> submittedVariants = (Collection<Document>)mergedVariant.get(SS_INFO_FIELD);

        for (Document submittedVariant : submittedVariants) {
            String reference = submittedVariant.getString(REFERENCE_ALLELE_FIELD);
            String alternate = submittedVariant.getString(ALTERNATE_ALLELE_FIELD);
            String study = submittedVariant.getString(STUDY_FIELD);
            boolean submittedVariantValidated = submittedVariant.getBoolean(VALIDATED_FIELD, DEFAULT_VALIDATED);
            boolean allelesMatch = submittedVariant.getBoolean(ALLELES_MATCH_FIELD, DEFAULT_ALLELES_MATCH);
            boolean assemblyMatch = submittedVariant.getBoolean(ASSEMBLY_MATCH_FIELD, DEFAULT_ASSEMBLY_MATCH);
            boolean evidence = submittedVariant.getBoolean(SUPPORTED_BY_EVIDENCE_FIELD, DEFAULT_SUPPORTED_BY_EVIDENCE);

            VariantSourceEntry sourceEntry = buildVariantSourceEntry(study, mergedInto, sequenceOntology, validated,
                                                                     submittedVariantValidated, allelesMatch,
                                                                     assemblyMatch, evidence);

            addToVariants(variants, contig, start, rs, reference, alternate, sourceEntry);
        }
        return new ArrayList<>(variants.values());
    }
}
