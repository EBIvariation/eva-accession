package uk.ac.ebi.eva.accession.clustering.batch.io.qc;

import com.google.common.collect.Sets;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class DuplicateRSAccQCProcessor implements ItemProcessor<List<Long>, List<DuplicateRSAccQCResult>> {
    private static final Logger logger = LoggerFactory.getLogger(DuplicateRSAccQCProcessor.class);

    private static final String ACCESSION_FIELD = "accession";
    private static final String SVE_RS_FIELD = "rs";
    private MongoTemplate mongoTemplate;

    public DuplicateRSAccQCProcessor(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public List<DuplicateRSAccQCResult> process(List<Long> cveAccessions) {
        // get all the CVE documents for the given accessions
        List<ClusteredVariantEntity> clusteredVariantEntitiesList = getClusteredVariantEntityList(cveAccessions);

        // Group CVE documents by accession
        Map<Long, List<ClusteredVariantEntity>> cveAccessionToEntitiesMap = clusteredVariantEntitiesList.stream()
                .collect(Collectors.groupingBy(ClusteredVariantEntity::getAccession));

        // Filter CVE accessions with more than one document
        Set<Long> cveAccessionsWithMultipleDocs = cveAccessionToEntitiesMap.entrySet().stream()
                .filter(entry -> entry.getValue().size() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        // get all the SVE documents belonging to the above CVE accessions and then group them
        Map<Long, Map<String, Set<SubmittedVariantEntity>>> sveGroupedDocuments = getAllSubmittedVariantEntitiesForCVEAccs(cveAccessionsWithMultipleDocs)
                .stream()
                .collect(Collectors.groupingBy(
                        // First level of grouping: by rsID
                        SubmittedVariantEntity::getClusteredVariantAccession,
                        // Second level of grouping: by assembly, contig, and position
                        Collectors.groupingBy(sve ->
                                        String.join("_", sve.getReferenceSequenceAccession(), sve.getContig(), String.valueOf(sve.getStart())),
                                Collectors.toSet()
                        )
                ));

        List<DuplicateRSAccQCResult> duplicateRSAccQCResultList = new ArrayList<>();

        /**
         Duplicate RS Accession defined in EVA-3671:
         @see <a href="https://www.ebi.ac.uk/panda/jira/browse/EVA-3671">Define duplicate for RS ids (Clustering)</a>

         To find a duplicate RS accession:
         i) get all the ssids belonging to that RS accession
         ii) group all of them into different sets by assembly, contig and position
         iii) check if all the sets form a linked chain in such a way that set1->intersect_>set2, set2->intersect->set3
         */
        sveGroupedDocuments.forEach((cveAcc, groupMap) -> {
            MutableGraph<String> graph = GraphBuilder.undirected().build();
            List<String> groupKeys = new ArrayList<>(groupMap.keySet());

            groupKeys.forEach(graph::addNode);

            for (int i = 0; i < groupKeys.size(); i++) {
                for (int j = i + 1; j < groupKeys.size(); j++) {
                    Set<Long> ssIdSet1 = groupMap.get(groupKeys.get(i)).stream()
                            .map(sve -> sve.getAccession())
                            .collect(Collectors.toSet());
                    Set<Long> ssIdSet2 = groupMap.get(groupKeys.get(j)).stream()
                            .map(sve -> sve.getAccession())
                            .collect(Collectors.toSet());

                    if (!Sets.intersection(ssIdSet1, ssIdSet2).isEmpty()) {
                        graph.putEdge(groupKeys.get(i), groupKeys.get(j));
                    }
                }
            }

            if (!isSingleConnectedComponent(graph)) {
                logger.warn("Found Duplicate RS Accession {}", cveAcc);
                duplicateRSAccQCResultList.add(new DuplicateRSAccQCResult(cveAcc, cveAccessionToEntitiesMap.get(cveAcc), groupMap));
            }
        });

        return duplicateRSAccQCResultList;
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
        Query query = query(where(ACCESSION_FIELD).in(cveAccessions));
        logger.info("Issuing find in EVA collections for CVEs containing the given CVE accessions : {}", query);
        List<ClusteredVariantEntity> evaResults = mongoTemplate.find(query, ClusteredVariantEntity.class);
        List<DbsnpClusteredVariantEntity> dbsnpResults = mongoTemplate.find(query, DbsnpClusteredVariantEntity.class);
        return Stream.concat(evaResults.stream(), dbsnpResults.stream()).collect(Collectors.toList());
    }

    private List<SubmittedVariantEntity> getAllSubmittedVariantEntitiesForCVEAccs(Set<Long> cveAccs) {
        Query query = query(where(SVE_RS_FIELD).in(cveAccs));
        logger.info("Issuing find in EVA collection for SVEs containing the given CVE accessions : {}", query);
        List<SubmittedVariantEntity> evaResults = mongoTemplate.find(query, SubmittedVariantEntity.class);
        List<DbsnpSubmittedVariantEntity> dbsnpResults = mongoTemplate.find(query, DbsnpSubmittedVariantEntity.class);
        return Stream.concat(evaResults.stream(), dbsnpResults.stream()).collect(Collectors.toList());
    }
}
