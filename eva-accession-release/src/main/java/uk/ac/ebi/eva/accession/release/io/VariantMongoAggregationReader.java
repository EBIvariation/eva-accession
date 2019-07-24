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
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.List;
import java.util.Map;

public abstract class VariantMongoAggregationReader implements ItemStreamReader<List<Variant>> {

    public static final String VARIANT_CLASS_KEY = "VC";

    public static final String STUDY_ID_KEY = "SID";

    public static final String CLUSTERED_VARIANT_VALIDATED_KEY = "RS_VALIDATED";

    public static final String SUBMITTED_VARIANT_VALIDATED_KEY = "SS_VALIDATED";

    public static final String ASSEMBLY_MATCH_KEY = "ASMM";

    public static final String ALLELES_MATCH_KEY = "ALMM";

    public static final String SUPPORTED_BY_EVIDENCE_KEY = "LOE";

    public static final String MERGED_INTO_KEY = "CURR";

    protected static final String DBSNP_SUBMITTED_VARIANT_ENTITY = "dbsnpSubmittedVariantEntity";

    protected static final String DBSNP_SUBMITTED_VARIANT_OPERATION_ENTITY = "dbsnpSubmittedVariantOperationEntity";

    protected static final String ACCESSION_FIELD = "accession";

    protected static final String REFERENCE_ASSEMBLY_FIELD = "asm";

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

    private static final String RS_PREFIX = "rs";

    protected String assemblyAccession;

    protected MongoClient mongoClient;

    protected String database;

    protected MongoCursor<Document> cursor;

    private int chunkSize;

    public VariantMongoAggregationReader(String assemblyAccession, MongoClient mongoClient, String database,
                                         int chunkSize) {
        this.assemblyAccession = assemblyAccession;
        this.mongoClient = mongoClient;
        this.database = database;
        this.chunkSize = chunkSize;
    }

    protected void aggregate(String collectionName) {
        MongoDatabase db = mongoClient.getDatabase(database);
        MongoCollection<Document> collection = db.getCollection(collectionName);
        AggregateIterable<Document> clusteredVariants = collection.aggregate(buildAggregation())
                                                                  .allowDiskUse(true)
                                                                  .useCursor(true)
                                                                  .batchSize(chunkSize);
        cursor = clusteredVariants.iterator();
    }

    abstract List<Bson> buildAggregation();

    @Override
    public List<Variant> read() throws UnexpectedInputException, ParseException, NonTransientResourceException {
        return cursor.hasNext() ? getVariants(cursor.next()) : null;
    }

    abstract List<Variant> getVariants(Document clusteredVariant);

    protected VariantSourceEntry buildVariantSourceEntry(String study, String sequenceOntology, boolean validated,
                                                         boolean submittedVariantValidated, boolean allelesMatch,
                                                         boolean assemblyMatch, boolean evidence, Long mergedInto) {
        VariantSourceEntry variantSourceEntry = buildVariantSourceEntry(study, sequenceOntology, validated,
                                                                        submittedVariantValidated, allelesMatch,
                                                                        assemblyMatch, evidence);
        variantSourceEntry.addAttribute(MERGED_INTO_KEY, buildId(mergedInto));
        return variantSourceEntry;
    }

    protected VariantSourceEntry buildVariantSourceEntry(String study, String sequenceOntology, boolean validated,
                                                         boolean submittedVariantValidated, boolean allelesMatch,
                                                         boolean assemblyMatch, boolean evidence) {
        VariantSourceEntry sourceEntry = new VariantSourceEntry(study, study);
        sourceEntry.addAttribute(VARIANT_CLASS_KEY, sequenceOntology);
        sourceEntry.addAttribute(STUDY_ID_KEY, study);
        sourceEntry.addAttribute(CLUSTERED_VARIANT_VALIDATED_KEY, Boolean.toString(validated));
        sourceEntry.addAttribute(SUBMITTED_VARIANT_VALIDATED_KEY, Boolean.toString(submittedVariantValidated));
        sourceEntry.addAttribute(ALLELES_MATCH_KEY, Boolean.toString(allelesMatch));
        sourceEntry.addAttribute(ASSEMBLY_MATCH_KEY, Boolean.toString(assemblyMatch));
        sourceEntry.addAttribute(SUPPORTED_BY_EVIDENCE_KEY, Boolean.toString(evidence));
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
}
