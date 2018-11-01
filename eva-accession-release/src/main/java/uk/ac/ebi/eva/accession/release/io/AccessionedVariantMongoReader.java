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
import java.util.List;
import java.util.Objects;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.orderBy;

public class AccessionedVariantMongoReader implements ItemStreamReader<Variant> {

    private static final String DBSNP_CLUSTERED_VARIANT_ENTITY = "dbsnpClusteredVariantEntity";

    private static final String DBSNP_SUBMITTED_VARIANT_ENTITY = "dbsnpSubmittedVariantEntity";

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
        Bson match = Aggregates.match(Filters.eq("asm", assemblyAccession));
        Bson lookup = Aggregates.lookup(DBSNP_SUBMITTED_VARIANT_ENTITY, "accession", "rs", "ssInfo");
        Bson sort = Aggregates.sort(orderBy(ascending("contig", "start")));
        return Arrays.asList(match, lookup, sort);
    }

    @Override
    public Variant read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return cursor.hasNext() ? getVariant(cursor.next()) : null;
    }

    Variant getVariant(Document clusteredVariant) {
        String contig = (String) clusteredVariant.get("contig");
        long start = (long) clusteredVariant.get("start");
        long rs = (long) clusteredVariant.get("accession");
        String reference = "";
        String alternate = "";
        long end = 0L;
        List<VariantSourceEntry> sourceEntries = new ArrayList<>();
        String type = (String) clusteredVariant.get("type");
        String sequenceOntology = VariantTypeToSOAccessionMap.getSequenceOntologyAccession(VariantType.valueOf(type));
        sourceEntries.add(new VariantSourceEntry(sequenceOntology, sequenceOntology));

        List<Document> submittedVariants = (List) clusteredVariant.get("ssInfo");
        for (Document submitedVariant : submittedVariants) {
            reference = (String) submitedVariant.get("ref");
            alternate = (String) submitedVariant.get("alt");
            end = calculateEnd(reference, alternate, start);
            String study = (String) submitedVariant.get("study");
            sourceEntries.add(new VariantSourceEntry(study, study));
        }

        Variant variant = new Variant(contig, start, end, reference, alternate);
        variant.setMainId(Objects.toString(rs));
        variant.addSourceEntries(sourceEntries);
        return variant;
    }

    private long calculateEnd(String reference, String alternate, long start) {
        long length = Math.max(reference.length(), alternate.length()) - 1;
        return start + length;
    }

    @Override
    public void close() throws ItemStreamException {
        cursor.close();
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
    }
}
