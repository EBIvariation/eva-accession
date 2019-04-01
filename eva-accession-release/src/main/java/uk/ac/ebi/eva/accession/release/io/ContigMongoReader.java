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

import java.util.Arrays;
import java.util.List;

public class ContigMongoReader implements ItemStreamReader<String> {

    private static final String CONTIG_KEY = "$contig";

    private static Logger logger = LoggerFactory.getLogger(ContigMongoReader.class);

    private static final String DBSNP_CLUSTERED_VARIANT_ENTITY = "dbsnpClusteredVariantEntity";

    public static final String VARIANT_CLASS_KEY = "VC";

    public static final String STUDY_ID_KEY = "SID";

    public static final String CLUSTERED_VARIANT_VALIDATED_KEY = "RS_VALIDATED";

    public static final String SUBMITTED_VARIANT_VALIDATED_KEY = "SS_VALIDATED";

    public static final String ASSEMBLY_MATCH_KEY = "ASMM";

    public static final String ALLELES_MATCH_KEY = "ALMM";

    public static final String SUPPORTED_BY_EVIDENCE_KEY = "LOE";

    public static final String MERGED_INTO_KEY = "CURR";

    protected static final String DBSNP_SUBMITTED_VARIANT_ENTITY = "dbsnpSubmittedVariantEntity";

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

    public ContigMongoReader(String assemblyAccession, MongoClient mongoClient, String database) {
        this.assemblyAccession = assemblyAccession;
        this.mongoClient = mongoClient;
        this.database = database;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        aggregate(DBSNP_CLUSTERED_VARIANT_ENTITY);
    }

    private void aggregate(String collectionName) {
        logger.debug("Preparing query to database {}, collection {}", database, collectionName);
        MongoDatabase db = mongoClient.getDatabase(database);
        MongoCollection<Document> collection = db.getCollection(collectionName);
        AggregateIterable<Document> clusteredVariants = collection.aggregate(buildAggregation())
                                                                  .allowDiskUse(true)
                                                                  .useCursor(true);
        cursor = clusteredVariants.iterator();
    }

    private List<Bson> buildAggregation() {
        Bson match = Aggregates.match(Filters.eq(REFERENCE_ASSEMBLY_FIELD, assemblyAccession));
        Bson uniqueContigs = Aggregates.group(CONTIG_KEY);
        List<Bson> aggregation = Arrays.asList(match, uniqueContigs);
        logger.info("Issuing aggregation: {}", aggregation);
        return aggregation;
    }


    @Override
    public String read() throws UnexpectedInputException, ParseException, NonTransientResourceException {
        return cursor.hasNext() ? getContig(cursor.next()) : null;
    }

    private String getContig(Document clusteredVariant) {
        return clusteredVariant.getString("_id");
    }

    @Override
    public void close() throws ItemStreamException {
        cursor.close();
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
    }
}
