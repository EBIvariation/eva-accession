package uk.ac.ebi.eva.accession.clustering.test;

import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;

import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class DatabaseState {
    Map<Long, List<ClusteredVariantEntity>> cveDbsnpCveEntries;
    Map<Long, List<SubmittedVariantEntity>> sveDbsnpSveEntries;
    Map<String, List<ClusteredVariantOperationEntity>> cvoeEntries;
    Map<String, List<DbsnpClusteredVariantOperationEntity>> dbsnpCvoeEntries;
    Map<String, List<SubmittedVariantOperationEntity>> svoeEntries;
    Map<String, List<DbsnpSubmittedVariantOperationEntity>> dbsnpSvoeEntries;

    public DatabaseState(
            Map<Long, List<ClusteredVariantEntity>> cveDbsnpCveEntries,
            Map<Long, List<SubmittedVariantEntity>> sveDbsnpSveEntries,
            Map<String, List<ClusteredVariantOperationEntity>> cvoeEntries,
            Map<String, List<DbsnpClusteredVariantOperationEntity>> dbsnpCvoeEntries,
            Map<String, List<SubmittedVariantOperationEntity>> svoeEntries,
            Map<String, List<DbsnpSubmittedVariantOperationEntity>> dbsnpSvoeEntries) {
        this.cveDbsnpCveEntries = cveDbsnpCveEntries;
        this.sveDbsnpSveEntries = sveDbsnpSveEntries;
        this.cvoeEntries = cvoeEntries;
        this.dbsnpCvoeEntries = dbsnpCvoeEntries;
        this.svoeEntries = svoeEntries;
        this.dbsnpSvoeEntries = dbsnpSvoeEntries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatabaseState that = (DatabaseState) o;
        return Objects.equals(cveDbsnpCveEntries, that.cveDbsnpCveEntries) && Objects.equals(
                sveDbsnpSveEntries, that.sveDbsnpSveEntries) && areOperationMapsEqual(cvoeEntries, that.cvoeEntries)
                && areOperationMapsEqual(dbsnpCvoeEntries, that.dbsnpCvoeEntries) &&
                areOperationMapsEqual(svoeEntries, that.svoeEntries) &&
                areOperationMapsEqual(dbsnpSvoeEntries, that.dbsnpSvoeEntries);
    }

    private <T> boolean areOperationMapsEqual(Map<String, List<T>> operation1, Map<String, List<T>> operation2) {
        boolean sameSize = (operation1.size() == operation2.size()) &&
                operation1.keySet().equals(operation2.keySet());
        if (!sameSize) {
            return false;
        }
        for(String key: operation1.keySet()) {
            // Getting just the first document is fine because the keys are "_id" in op collections
            // Therefore, there will only be one document per ID
            if (!areOperationsEqual((EventDocument) operation1.get(key).get(0),
                                    (EventDocument) operation2.get(key).get(0))) {
                return false;
            }
        }
        return true;
    }

    // We need this because the EventDocument class in accession-commons does not provide an equals method
    private boolean areOperationsEqual(EventDocument operation1, EventDocument operation2) {
        return Objects.equals(operation1.getId(), operation2.getId()) &&
                operation1.getEventType() == operation2.getEventType() &&
                Objects.equals(operation1.getAccession(), operation2.getAccession()) &&
                Objects.equals(operation1.getMergedInto(), operation2.getMergedInto()) &&
                Objects.equals(operation1.getSplitInto(), operation2.getSplitInto()) &&
                Objects.equals(operation1.getReason(), operation2.getReason()) &&
                Objects.equals(operation1.getInactiveObjects(), operation2.getInactiveObjects()) &&
                Objects.equals(operation1.getCreatedDate(), operation2.getCreatedDate());
    }

    public static DatabaseState getCurrentDatabaseState(MongoTemplate mongoTemplate) {
        List<ClusteredVariantEntity> tempCVEArray = mongoTemplate.findAll(ClusteredVariantEntity.class);
        tempCVEArray.addAll(mongoTemplate.findAll(DbsnpClusteredVariantEntity.class));
        Map<Long, List<ClusteredVariantEntity>> cveDbsnpCveEntries =
                tempCVEArray.stream().collect(Collectors.groupingBy(ClusteredVariantEntity::getAccession));
        List<SubmittedVariantEntity> tempSVEArray = mongoTemplate.findAll(SubmittedVariantEntity.class);
        tempSVEArray.addAll(mongoTemplate.findAll(DbsnpSubmittedVariantEntity.class));
        Map<Long, List<SubmittedVariantEntity>> sveDbsnpSveEntries =
                tempSVEArray.stream().collect(Collectors.groupingBy(SubmittedVariantEntity::getAccession));
        Map<String, List<ClusteredVariantOperationEntity>> cvoeEntries =
                mongoTemplate.findAll(ClusteredVariantOperationEntity.class)
                                  .stream().collect(Collectors.groupingBy(ClusteredVariantOperationEntity::getId));
        Map<String, List<DbsnpClusteredVariantOperationEntity>> dbsnpCvoeEntries =
                mongoTemplate.findAll(DbsnpClusteredVariantOperationEntity.class)
                                  .stream().collect(Collectors.groupingBy(DbsnpClusteredVariantOperationEntity::getId));
        Map<String, List<SubmittedVariantOperationEntity>> svoeEntries =
                mongoTemplate.findAll(SubmittedVariantOperationEntity.class)
                                  .stream().collect(Collectors.groupingBy(SubmittedVariantOperationEntity::getId));
        Map<String, List<DbsnpSubmittedVariantOperationEntity>> dbsnpSvoeEntries =
                mongoTemplate.findAll(DbsnpSubmittedVariantOperationEntity.class)
                                  .stream().collect(Collectors.groupingBy(DbsnpSubmittedVariantOperationEntity::getId));

        return new DatabaseState(cveDbsnpCveEntries, sveDbsnpSveEntries, cvoeEntries, dbsnpCvoeEntries,
                                 svoeEntries, dbsnpSvoeEntries);
    }
}
