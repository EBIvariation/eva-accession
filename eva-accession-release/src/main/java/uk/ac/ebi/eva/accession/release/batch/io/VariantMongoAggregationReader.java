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

package uk.ac.ebi.eva.accession.release.batch.io;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import uk.ac.ebi.eva.accession.release.collectionNames.CollectionNames;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class VariantMongoAggregationReader implements ItemStreamReader<List<Variant>> {

    private static final Logger logger = LoggerFactory.getLogger(VariantMongoAggregationReader.class);

    public static final String VARIANT_CLASS_KEY = "VC";

    public static final String STUDY_ID_KEY = "SID";

    public static final String CLUSTERED_VARIANT_VALIDATED_KEY = "RS_VALIDATED";

    public static final String SUBMITTED_VARIANT_VALIDATED_KEY = "SS_VALIDATED";

    public static final String ASSEMBLY_MATCH_KEY = "ASMM";

    public static final String ALLELES_MATCH_KEY = "ALMM";

    public static final String REMAPPED_KEY = "REMAPPED";

    public static final String SUPPORTED_BY_EVIDENCE_KEY = "LOE";

    public static final String MERGED_INTO_KEY = "CURR";

    public static final String MAPPING_WEIGHT_KEY = "MAP_WEIGHT";

    protected static final String ACCESSION_FIELD = "accession";

    protected static final String REFERENCE_ASSEMBLY_FIELD = "asm";

    protected static final String TAXONOMY_FIELD = "tax";

    protected static final String REFERENCE_ASSEMBLY_FIELD_IN_SUBMITTED_COLLECTIONS = "seq";

    protected static final String STUDY_FIELD = "study";

    protected static final String CONTIG_FIELD = "contig";

    protected static final String START_FIELD = "start";

    protected static final String TYPE_FIELD = "type";

    protected static final String REFERENCE_ALLELE_FIELD = "ref";

    protected static final String ALTERNATE_ALLELE_FIELD = "alt";

    protected static final String CLUSTERED_VARIANT_ACCESSION_FIELD = "rs";

    protected static final String SS_INFO_FIELD = "ssInfo";

    protected static final String VALIDATED_FIELD = "validated";

    protected static final String ASSEMBLY_MATCH_FIELD = "asmMatch";

    protected static final String ALLELES_MATCH_FIELD = "allelesMatch";

    protected static final String SUPPORTED_BY_EVIDENCE_FIELD = "evidence";

    public static final String MAPPING_WEIGHT_FIELD = "mapWeight";

    private static final String RS_PREFIX = "rs";

    protected String assemblyAccession;

    protected int taxonomyAccession;

    protected MongoClient mongoClient;

    protected String database;

    protected MongoCursor<Document> cursor;

    private int chunkSize;

    protected CollectionNames names;

    public VariantMongoAggregationReader(String assemblyAccession, int taxonomyAccession, MongoClient mongoClient,
                                         String database, int chunkSize, CollectionNames names) {
        this.assemblyAccession = assemblyAccession;
        this.taxonomyAccession = taxonomyAccession;
        this.mongoClient = mongoClient;
        this.database = database;
        this.chunkSize = chunkSize;
        this.names = names;
    }

    protected void aggregate(String collectionName) {
        MongoDatabase db = mongoClient.getDatabase(database);
        logger.info("issuing aggregation on collection {}", collectionName);
        MongoCollection<Document> collection = db.getCollection(collectionName);
        AggregateIterable<Document> clusteredVariants = collection.aggregate(buildAggregation())
                                                                  .allowDiskUse(true)
                                                                  .useCursor(true)
                                                                  .batchSize(chunkSize);
        cursor = clusteredVariants.iterator();
    }

    abstract protected List<Bson> buildAggregation();

    @Override
    public List<Variant> read() throws Exception {
        return cursor.hasNext() ? getVariants(cursor.next()) : null;
    }

    abstract protected List<Variant> getVariants(Document clusteredVariant);

    protected VariantSourceEntry buildVariantSourceEntry(String study, String sequenceOntology, boolean validated,
                                                         boolean submittedVariantValidated, boolean allelesMatch,
                                                         boolean assemblyMatch, boolean evidence, boolean remappedRS,
                                                         Long mergedInto) {
        VariantSourceEntry variantSourceEntry = buildVariantSourceEntry(study, sequenceOntology, validated,
                                                                        submittedVariantValidated, allelesMatch,
                                                                        assemblyMatch, evidence, remappedRS);
        if (Objects.nonNull(mergedInto)) {
            variantSourceEntry.addAttribute(MERGED_INTO_KEY, buildId(mergedInto));
        }
        return variantSourceEntry;
    }

    protected VariantSourceEntry buildVariantSourceEntry(String study, String sequenceOntology, boolean validated,
                                                         boolean submittedVariantValidated, boolean allelesMatch,
                                                         boolean assemblyMatch, boolean evidence,
                                                         boolean remappedRS) {
        VariantSourceEntry sourceEntry = new VariantSourceEntry(study, study);
        sourceEntry.addAttribute(VARIANT_CLASS_KEY, sequenceOntology);
        sourceEntry.addAttribute(STUDY_ID_KEY, study);
        sourceEntry.addAttribute(CLUSTERED_VARIANT_VALIDATED_KEY, Boolean.toString(validated));
        sourceEntry.addAttribute(SUBMITTED_VARIANT_VALIDATED_KEY, Boolean.toString(submittedVariantValidated));
        sourceEntry.addAttribute(ALLELES_MATCH_KEY, Boolean.toString(allelesMatch));
        sourceEntry.addAttribute(ASSEMBLY_MATCH_KEY, Boolean.toString(assemblyMatch));
        sourceEntry.addAttribute(SUPPORTED_BY_EVIDENCE_KEY, Boolean.toString(evidence));
        sourceEntry.addAttribute(REMAPPED_KEY, Boolean.toString(remappedRS));
        return sourceEntry;
    }

    private String buildId(long rs) {
        return RS_PREFIX + rs;
    }

    protected void addToVariants(Map<String, Variant> variants, String contig, long start, long rs, String reference,
                                 String alternate, VariantSourceEntry sourceEntry) {
        String variantId = (contig + "_" + start + "_" + reference + "_" + alternate).toUpperCase();
        if (variants.containsKey(variantId)) {
            variants.get(variantId).addSourceEntry(sourceEntry);
        } else {
            long end = calculateEnd(reference, alternate, start);
            Variant variant = new Variant(contig, start, end, reference, alternate);
            variant.setMainId(buildId(rs));
            variant.setIds(Collections.singleton(buildId(rs)));
            variant.addSourceEntry(sourceEntry);
            variants.put(variantId, variant);
        }
    }

    private long calculateEnd(String reference, String alternate, long start) {
        long length = Math.max(reference.length(), alternate.length());
        return start + length - 1;
    }

    @Override
    public void close() throws ItemStreamException {
        cursor.close();
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
    }

    /**
     * The query performed in mongo can retrieve more variants than the actual ones because in some cases the same
     * clustered variant is mapped against multiple locations. So we need to check that that clustered variant we are
     * processing only appears in the VCF release file with the alleles from submitted variants matching the location.
     */
    protected boolean isSameLocation(String contig, long start, String submittedVariantContig,
                                     long submittedVariantStart,
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

    public int getChunkSize() {
        return chunkSize;
    }
}
