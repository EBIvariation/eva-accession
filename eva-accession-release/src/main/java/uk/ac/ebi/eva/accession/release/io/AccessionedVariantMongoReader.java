package uk.ac.ebi.eva.accession.release.io;

import com.mongodb.BasicDBObject;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.LookupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.query.Criteria;

import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregationOptions;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.sort;

public class AccessionedVariantMongoReader implements ItemReader<Variant> {

    private MongoTemplate mongoTemplate;

    private AggregationResults<BasicDBObject> clusteredVariants;

    public AccessionedVariantMongoReader(MongoTemplate mongoTemplate){
        this.mongoTemplate = mongoTemplate;
        initializeReader();
    }

    private void initializeReader() {
        Aggregation aggregation = buildAggregation();
        clusteredVariants = mongoTemplate.aggregate(aggregation, "dbsnpClusteredVariantEntity", BasicDBObject.class);
    }

    private Aggregation buildAggregation() {
        MatchOperation matchOperation = match(Criteria.where("asm").is("GCF_000409795.2"));

        LookupOperation lookupOperation = LookupOperation.newLookup()
                                                         .from("dbsnpSubmittedVariantEntity")
                                                         .localField("accession")
                                                         .foreignField("rs")
                                                         .as("ssInfo");

        SortOperation sortOperation = sort(Sort.Direction.ASC, "contig", "start");

        AggregationOptions options = newAggregationOptions().allowDiskUse(true).build();

        return newAggregation(matchOperation, lookupOperation, sortOperation).withOptions(options);
    }

    @Override
    public Variant read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return getVariant(clusteredVariants.getMappedResults().get(0));
    }

    private Variant getVariant(BasicDBObject clusteredVariant) {
        String contig = (String) clusteredVariant.get("contig");
        long start = (long) clusteredVariant.get("start");
        long rs = (long) clusteredVariant.get("accession");
        String reference = "";
        String alternate = "";
        long end = 0L;
        List<VariantSourceEntry> studies = new ArrayList<>();

        List<BasicDBObject> submittedVariants = (List) clusteredVariant.get("ssInfo");
        for (BasicDBObject submitedVariant : submittedVariants) {
            reference = (String) submitedVariant.get("ref");
            alternate = (String) submitedVariant.get("alt");
            end = calculateEnd(reference, alternate, start);
            String study = (String) submitedVariant.get("study");
            studies.add(new VariantSourceEntry(study, study));
        }

        Variant variant = new Variant(contig, start, end, reference, alternate);
        variant.setMainId(Objects.toString(rs));
        variant.addSourceEntries(studies);
        return variant;
    }

    private long calculateEnd(String reference, String alternate, long start) {
        long referenceLength = reference.length() - 1;
        long alternateLength = alternate.length() - 1;
        return (referenceLength >= alternateLength) ? start + referenceLength : start + alternateLength;
    }
}
