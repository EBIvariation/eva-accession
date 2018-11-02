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
import java.util.List;
import java.util.Objects;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.orderBy;

public class AccessionedVariantMongoReader implements ItemStreamReader<Variant> {

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
        return Arrays.asList(match, lookup, sort);
    }

    @Override
    public Variant read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return cursor.hasNext() ? getVariant(cursor.next()) : null;
    }

    Variant getVariant(Document clusteredVariant) {
        String contig = clusteredVariant.getString(CONTIG_FIELD);
        long start = clusteredVariant.getLong(START_FIELD);
        long rs = clusteredVariant.getLong(ACCESSION_FIELD);
        String reference = "";
        String alternate = "";
        long end = start;
        List<VariantSourceEntry> sourceEntries = new ArrayList<>();
        String type = clusteredVariant.getString(TYPE_FIELD);
        String sequenceOntology = VariantTypeToSOAccessionMap.getSequenceOntologyAccession(VariantType.valueOf(type));

        Collection<Document> submittedVariants = (Collection<Document>)clusteredVariant.get(SS_INFO_FIELD);
        for (Document submitedVariant : submittedVariants) {
            reference = submitedVariant.getString(REFERENCE_ALLELE_FIELD);
            alternate = submitedVariant.getString(ALTERNATE_ALLELE_FIELD);
            end = calculateEnd(reference, alternate, start);
            String study = submitedVariant.getString(STUDY_FIELD);
            sourceEntries.add(buildVariantSourceEntry(study, sequenceOntology));
        }

        Variant variant = new Variant(contig, start, end, reference, alternate);
        variant.setMainId(Objects.toString(rs));
        variant.addSourceEntries(sourceEntries);
        return variant;
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

    @Override
    public void close() throws ItemStreamException {
        cursor.close();
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
    }
}
