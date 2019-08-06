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

import uk.ac.ebi.eva.accession.core.SubmittedVariant;
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

public class AccessionedVariantMongoReader extends VariantMongoAggregationReader {

    private static final Logger logger = LoggerFactory.getLogger(AccessionedVariantMongoReader.class);

    private static final String DBSNP_CLUSTERED_VARIANT_ENTITY = "dbsnpClusteredVariantEntity";

    public AccessionedVariantMongoReader(String assemblyAccession, MongoClient mongoClient, String database,
                                         int chunkSize) {
        super(assemblyAccession, mongoClient, database, chunkSize);
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        aggregate(DBSNP_CLUSTERED_VARIANT_ENTITY);
    }

    List<Bson> buildAggregation() {
        Bson match = Aggregates.match(Filters.eq(REFERENCE_ASSEMBLY_FIELD, assemblyAccession));
        Bson sort = Aggregates.sort(orderBy(ascending(CONTIG_FIELD, START_FIELD)));
        Bson lookup = Aggregates.lookup(DBSNP_SUBMITTED_VARIANT_ENTITY, ACCESSION_FIELD,
                                        CLUSTERED_VARIANT_ACCESSION_FIELD, SS_INFO_FIELD);
        List<Bson> aggregation = Arrays.asList(match, sort, lookup);
        logger.info("Issuing aggregation: {}", aggregation);
        return aggregation;
    }

    List<Variant> getVariants(Document clusteredVariant) {
        String contig = clusteredVariant.getString(CONTIG_FIELD);
        long start = clusteredVariant.getLong(START_FIELD);
        long rs = clusteredVariant.getLong(ACCESSION_FIELD);
        String type = clusteredVariant.getString(TYPE_FIELD);
        String sequenceOntology = VariantTypeToSOAccessionMap.getSequenceOntologyAccession(VariantType.valueOf(type));
        boolean validated = clusteredVariant.getBoolean(VALIDATED_FIELD, DEFAULT_VALIDATED);

        Map<String, Variant> variants = new HashMap<>();
        Collection<Document> submittedVariants = (Collection<Document>)clusteredVariant.get(SS_INFO_FIELD);

        for (Document submittedVariant : submittedVariants) {
            if (!isSameLocation(contig, start, submittedVariant)) {
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
                                                                     assemblyMatch, evidence);

            addToVariants(variants, contig, start, rs, reference, alternate, sourceEntry);
        }
        return new ArrayList<>(variants.values());
    }

    /**
     * The query performed in mongo can retrieve more variants than the actual ones because in some cases the same
     * clustered variant is mapped against multiple locations. So we need to check that that clustered variant we are
     * processing only appears in the VCF release file with the alleles from submitted variants matching the location.
     */
    private boolean isSameLocation(String contig, long start, Document submittedVariant) {
        long submittedVariantStart = submittedVariant.getLong(START_FIELD);
        String submittedVariantContig = submittedVariant.getString(CONTIG_FIELD);
        return contig.equals(submittedVariantContig) && isSameStart(start, submittedVariantStart);
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
    private boolean isSameStart(long clusteredVariantStart, long submittedVariantStart) {
         return clusteredVariantStart == submittedVariantStart
                 || Math.abs(clusteredVariantStart - submittedVariantStart) == 1L;
    }
}
