package uk.ac.ebi.eva.accession.release.io;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
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

import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.orderBy;

public class AccessionedVariantMongoReader implements ItemStreamReader<Variant> {

    private MongoCursor<Document> cursor;

    public AccessionedVariantMongoReader(){
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27017));
        MongoDatabase db = mongoClient.getDatabase("test-db");

        MongoCollection<Document> collection = db.getCollection("dbsnpClusteredVariantEntity");
        AggregateIterable<Document> clusteredVariants = collection.aggregate(buildAggregation())
                                                                  .allowDiskUse(true)
                                                                  .useCursor(true);
        cursor = clusteredVariants.iterator();
    }

    private List<Bson> buildAggregation() {
//        Bson match = Aggregates.match(Filters.eq("asm", "GCF_000001215.2")); //fruitfly 5M rs
//        Bson match = Aggregates.match(Filters.eq("asm", "GCF_000409795.2")); //green monkey 500K rs
//        Bson match = Aggregates.match(Filters.eq("asm", "GCF_000309985.1")); //field mustard 164 rs
        Bson match = Aggregates.match(Filters.eq("asm", "GCF_000409795.2"));
        Bson lookup = Aggregates.lookup("dbsnpSubmittedVariantEntity", "accession", "rs", "ssInfo");
        Bson sort = Aggregates.sort(orderBy(ascending("contig", "start")));
        return Arrays.asList(match, lookup, sort);
    }

    @Override
    public Variant read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return cursor.hasNext() ? getVariant(cursor.next()) : null;
    }

    private Variant getVariant(Document clusteredVariant) {
        String contig = (String) clusteredVariant.get("contig");
        long start = (long) clusteredVariant.get("start");
        long rs = (long) clusteredVariant.get("accession");
        String reference = "";
        String alternate = "";
        long end = 0L;
        List<VariantSourceEntry> studies = new ArrayList<>();

        List<Document> submittedVariants = (List) clusteredVariant.get("ssInfo");
        for (Document submitedVariant : submittedVariants) {
            reference = (String) submitedVariant.get("ref");
            alternate = (String) submitedVariant.get("alt");
            end = calculateEnd(reference, alternate, start);
            String study = (String) submitedVariant.get("study");
            studies.add(new VariantSourceEntry(study, study));
            //TODO: Add variant class
        }

        Variant variant = new Variant(contig, start, end, reference, alternate);
        variant.setMainId(Objects.toString(rs));
        variant.addSourceEntries(studies);
        return variant;
    }

    long calculateEnd(String reference, String alternate, long start) {
        long referenceLength = reference.length() - 1;
        long alternateLength = alternate.length() - 1;
        return (referenceLength >= alternateLength) ? start + referenceLength : start + alternateLength;
    }

    @Override
    public void close() throws ItemStreamException {
        cursor.close();
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }
}
