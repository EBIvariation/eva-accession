package uk.ac.ebi.eva.accession.pipeline.batch.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.pipeline.batch.io.DuplicateSSAccQCResult;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class DuplicateSSAccQCProcessor implements ItemProcessor<List<Long>, List<DuplicateSSAccQCResult>> {
    private static final Logger logger = LoggerFactory.getLogger(DuplicateSSAccQCProcessor.class);

    private static final String ACCESSION_FIELD = "accession";
    private static final String REMAPPED_FROM_FIELD = "remappedFrom";
    private MongoTemplate mongoTemplate;

    public DuplicateSSAccQCProcessor(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public List<DuplicateSSAccQCResult> process(List<Long> sveAccessions) {
        // get all the SVE documents for the given accessions
        List<SubmittedVariantEntity> submittedVariantEntitiesList = getSubmittedVariantEntityList(sveAccessions);

        // Group SVE documents by accession and filter those which has more than one document for accession
        List<DuplicateSSAccQCResult> duplicateSSAccQCResultList = submittedVariantEntitiesList.stream()
                .collect(Collectors.groupingBy(SubmittedVariantEntity::getAccession))
                .entrySet().stream()
                .filter(e -> e.getValue().size() > 1)
                .map(e -> new DuplicateSSAccQCResult(e.getKey(), e.getValue()))
                .collect(Collectors.toList());

        return duplicateSSAccQCResultList;
    }

    private List<SubmittedVariantEntity> getSubmittedVariantEntityList(List<Long> sveAccessions) {
        Query query = query(where(ACCESSION_FIELD).in(sveAccessions).and(REMAPPED_FROM_FIELD).exists(false));
        logger.info("Issuing find in EVA collection for SVEs containing the given accessions : {}", query);
        List<SubmittedVariantEntity> evaResults = mongoTemplate.find(query, SubmittedVariantEntity.class);
        List<DbsnpSubmittedVariantEntity> dbsnpResults = mongoTemplate.find(query, DbsnpSubmittedVariantEntity.class);
        return Stream.concat(evaResults.stream(), dbsnpResults.stream()).collect(Collectors.toList());
    }
}
