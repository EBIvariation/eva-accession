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
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
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

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.orderBy;

public class AccessionedVariantMongoReader implements ItemStreamReader<List<Variant>> {

    private static final Logger logger = LoggerFactory.getLogger(AccessionedVariantMongoReader.class);

    private static final String DBSNP_CLUSTERED_VARIANT_ENTITY = "dbsnpClusteredVariantEntity";

    private static final String DBSNP_SUBMITTED_VARIANT_ENTITY = "dbsnpSubmittedVariantEntity";

    private static final String ACCESSION_FIELD = "accession";

    private static final String REFERENCE_ASSEMBLY_FIELD = "asm";

    private static final String STUDY_FIELD = "study";

    private static final String CONTIG_FIELD = "contig";

    private static final String START_FIELD = "start";

    private static final String TYPE_FIELD = "type";

    private static final String REFERENCE_ALLELE_FIELD = "ref";

    private static final String ALTERNATE_ALLELE_FIELD = "alt";

    private static final String CLUSTERED_VARIANT_ACCESSION_FIELD = "rs";

    private static final String SS_INFO_FIELD = "ssInfo";

    static final String VARIANT_CLASS_KEY = "VC";

    static final String STUDY_ID_KEY = "SID";

    private static final String RS_PREFIX = "rs";

    private String assemblyAccession;

    private MongoClient mongoClient;

    private String database;

    private MongoCursor<Document> cursor;

    public AccessionedVariantMongoReader(String assemblyAccession, MongoClient mongoClient,
                                         String database) {
        this.assemblyAccession = assemblyAccession;
        this.mongoClient = mongoClient;
        this.database = database;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        MongoDatabase db = mongoClient.getDatabase(database);
        MongoCollection<Document> collection = db.getCollection(DBSNP_CLUSTERED_VARIANT_ENTITY);
        AggregateIterable<Document> clusteredVariants = collection.aggregate(buildAggregation())
                                                                  .allowDiskUse(true)
                                                                  .useCursor(true);
        cursor = clusteredVariants.iterator();
    }

    List<Bson> buildAggregation() {
        Bson match = Aggregates.match(Filters.eq(REFERENCE_ASSEMBLY_FIELD, assemblyAccession));
        Bson lookup = Aggregates.lookup(DBSNP_SUBMITTED_VARIANT_ENTITY, ACCESSION_FIELD,
                                        CLUSTERED_VARIANT_ACCESSION_FIELD, SS_INFO_FIELD);
        Bson sort = Aggregates.sort(orderBy(ascending(CONTIG_FIELD, START_FIELD)));
        List<Bson> aggregation = Arrays.asList(match, lookup, sort);
        logger.info("Issuing aggregation: {}", aggregation);
        return aggregation;
    }

    @Override
    public List<Variant> read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return cursor.hasNext() ? getVariants(cursor.next()) : null;
    }

    List<Variant> getVariants(Document clusteredVariant) {
        String contig = clusteredVariant.getString(CONTIG_FIELD);
        long start = clusteredVariant.getLong(START_FIELD);
        long rs = clusteredVariant.getLong(ACCESSION_FIELD);
        String type = clusteredVariant.getString(TYPE_FIELD);
        String sequenceOntology = VariantTypeToSOAccessionMap.getSequenceOntologyAccession(VariantType.valueOf(type));

        Map<String, Variant> variants = new HashMap<>();
        Collection<Document> submittedVariants = (Collection<Document>)clusteredVariant.get(SS_INFO_FIELD);
        for (Document submittedVariant : submittedVariants) {
            String reference = submittedVariant.getString(REFERENCE_ALLELE_FIELD);
            String alternate = submittedVariant.getString(ALTERNATE_ALLELE_FIELD);
            long end = calculateEnd(reference, alternate, start);
            String study = submittedVariant.getString(STUDY_FIELD);
            VariantSourceEntry sourceEntry = buildVariantSourceEntry(study, sequenceOntology);

            String variantId = (contig + "_" + start + "_" + reference + "_" + alternate).toUpperCase();
            if (variants.containsKey(variantId)) {
                variants.get(variantId).addSourceEntry(sourceEntry);
            } else {
                Variant variant = new Variant(contig, start, end, reference, alternate);
                variant.setMainId(buildId(rs));
                variant.addSourceEntry(sourceEntry);
                variants.put(variantId, variant);
            }
        }
        return new ArrayList<>(variants.values());
    }

    private long calculateEnd(String reference, String alternate, long start) {
        long length = Math.max(reference.length(), alternate.length());
        return start + length - 1;
    }

    private VariantSourceEntry buildVariantSourceEntry(String study, String sequenceOntology) {
        VariantSourceEntry sourceEntry = new VariantSourceEntry(study, study);
        sourceEntry.addAttribute(VARIANT_CLASS_KEY, sequenceOntology);
        sourceEntry.addAttribute(STUDY_ID_KEY, study);
        return sourceEntry;
    }

    private String buildId(long rs) {
        return RS_PREFIX + Objects.toString(rs);
    }

    @Override
    public void close() throws ItemStreamException {
        cursor.close();
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
    }
}
