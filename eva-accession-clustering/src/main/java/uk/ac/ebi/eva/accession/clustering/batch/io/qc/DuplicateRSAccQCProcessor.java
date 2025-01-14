package uk.ac.ebi.eva.accession.clustering.batch.io.qc;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DuplicateRSAccQCProcessor implements ItemProcessor<List<Long>, List<Long>> {
    private static final Logger logger = LoggerFactory.getLogger(DuplicateRSAccQCProcessor.class);

    private static final String ACCESSION_FIELD = "accession";
    private static final String SVE_RS_FIELD = "rs";
    private MongoTemplate mongoTemplate;
    private MongoConverter converter;

    public DuplicateRSAccQCProcessor(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
        this.converter = mongoTemplate.getConverter();
    }

    @Override
    public List<Long> process(List<Long> cveAccessions) {
        // get all the CVE documents for the given accessions
        List<ClusteredVariantEntity> clusteredVariantEntitiesList = getClusteredVariantEntityList(cveAccessions);

        // filter CVE accessions with more than one document
        Set<Long> cveAccessionsWithMultipleDocs = clusteredVariantEntitiesList.stream()
                .collect(Collectors.groupingBy(ClusteredVariantEntity::getAccession, Collectors.counting()))
                .entrySet().stream()
                .filter(entry -> entry.getValue() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        // get all the SVE documents belonging to the above CVE accessions and then group them
        Map<Long, Map<String, List<SubmittedVariantEntity>>> sveGroupedDocuments = getAllSubmittedVariantEntitiesForCVEAccs(cveAccessionsWithMultipleDocs)
                .stream()
                .collect(Collectors.groupingBy(
                        // First level of grouping: by rsID
                        SubmittedVariantEntity::getClusteredVariantAccession,
                        // Second level of grouping: by assembly, contig, and position
                        Collectors.groupingBy(sve ->
                                String.join("_", sve.getReferenceSequenceAccession(), sve.getContig(), String.valueOf(sve.getStart()))
                        )
                ));

        List<Long> duplicateCVEAccessions = new ArrayList<>();

        // find if a CVE accession is duplicate by processing its SVE docs grouped by assembly, contig and position
        sveGroupedDocuments.forEach((cveAcc, groupMap) -> {
            MutableGraph<String> graph = GraphBuilder.undirected().build();
            List<String> groupKeys = new ArrayList<>(groupMap.keySet());

            groupKeys.forEach(graph::addNode);

            for (int i = 0; i < groupKeys.size(); i++) {
                for (int j = i + 1; j < groupKeys.size(); j++) {
                    List<SubmittedVariantEntity> list1 = groupMap.get(groupKeys.get(i));
                    List<SubmittedVariantEntity> list2 = groupMap.get(groupKeys.get(j));
                    if (listsIntersect(list1, list2)) {
                        graph.putEdge(groupKeys.get(i), groupKeys.get(j));
                    }
                }
            }

            if (!isSingleConnectedComponent(graph)) {
                duplicateCVEAccessions.add(cveAcc);
            }
        });

        return duplicateCVEAccessions;
    }

    private boolean listsIntersect(List<SubmittedVariantEntity> list1, List<SubmittedVariantEntity> list2) {
        return list1.stream()
                .map(SubmittedVariantEntity::getAccession)
                .anyMatch(field -> list2.stream()
                        .map(SubmittedVariantEntity::getAccession)
                        .anyMatch(field::equals));
    }

    private boolean isSingleConnectedComponent(MutableGraph<String> graph) {
        if (graph.nodes().isEmpty()) {
            return true;
        }

        Set<String> visited = new HashSet<>();
        String startNode = graph.nodes().iterator().next();
        traverse(graph, startNode, visited);

        return visited.size() == graph.nodes().size();
    }

    private void traverse(MutableGraph<String> graph, String node, Set<String> visited) {
        if (visited.contains(node)) {
            return;
        }
        visited.add(node);
        for (String neighbor : graph.adjacentNodes(node)) {
            traverse(graph, neighbor, visited);
        }
    }

    private List<ClusteredVariantEntity> getClusteredVariantEntityList(List<Long> cveAccessions) {
        Bson query = Filters.and(Filters.in(ACCESSION_FIELD, cveAccessions));
        logger.info("Issuing find in EVA collection for a bunch of CVE containing the given CVE accs : {}", query);
        List<ClusteredVariantEntity> clusteredVariantEntitiesList = mongoTemplate.getCollection(mongoTemplate.getCollectionName(ClusteredVariantEntity.class))
                .find(query)
                .noCursorTimeout(true)
                .into(new ArrayList<>())
                .stream()
                .map(doc -> getClusteredVariantEntity(doc))
                .collect(Collectors.toList());

        return clusteredVariantEntitiesList;
    }

    private ClusteredVariantEntity getClusteredVariantEntity(Document document) {
        return converter.read(ClusteredVariantEntity.class, new BasicDBObject(document));
    }

    private List<SubmittedVariantEntity> getAllSubmittedVariantEntitiesForCVEAccs(Set<Long> cveAccs) {
        Bson query = Filters.and(Filters.in(SVE_RS_FIELD, cveAccs));
        logger.info("Issuing find in EVA collection for a bunch of SVE containing the given CVE accs : {}", query);
        List<SubmittedVariantEntity> submittedVariantEntitiesList = mongoTemplate.getCollection(mongoTemplate.getCollectionName(SubmittedVariantEntity.class))
                .find(query)
                .noCursorTimeout(true)
                .into(new ArrayList<>())
                .stream()
                .map(doc -> getSubmittedVariantEntity(doc))
                .collect(Collectors.toList());

        return submittedVariantEntitiesList;
    }

    private SubmittedVariantEntity getSubmittedVariantEntity(Document document) {
        return converter.read(SubmittedVariantEntity.class, new BasicDBObject(document));
    }
}
