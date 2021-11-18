package uk.ac.ebi.eva.accession.core.service.nonhuman;

import uk.ac.ebi.ampt2d.commons.accession.core.models.IEvent;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpClusteredVariantInactiveService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.ClusteredVariantInactiveService;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ClusteredVariantOperationService {

    private final DbsnpClusteredVariantInactiveService dbsnpClusteredVariantInactiveService;
    private final ClusteredVariantInactiveService clusteredVariantInactiveService;


    public ClusteredVariantOperationService(DbsnpClusteredVariantInactiveService dbsnpClusteredVariantInactiveService,
                                            ClusteredVariantInactiveService clusteredVariantInactiveService) {
        this.dbsnpClusteredVariantInactiveService = dbsnpClusteredVariantInactiveService;
        this.clusteredVariantInactiveService = clusteredVariantInactiveService;
    }

    public List<IEvent<? extends IClusteredVariant, Long>> getAllOperations(Long accession) {
        List<IEvent<? extends IClusteredVariant, Long>> totalOperations = new ArrayList<>();
        totalOperations.addAll(dbsnpClusteredVariantInactiveService.getAllEventsInvolvedIn(accession));
        totalOperations.addAll(clusteredVariantInactiveService.getAllEventsInvolvedIn(accession));
        sortOperationsOldToNew(totalOperations);
        return totalOperations;
    }

    private void sortOperationsOldToNew(List<IEvent<? extends IClusteredVariant, Long>> operations){
        operations.sort(Comparator.comparing(IEvent::getCreatedDate));
    }

    private void sortOperationsNewToOld(List<IEvent<? extends IClusteredVariant, Long>> operations) {
        operations.sort(Comparator.comparing(IEvent::getCreatedDate, Comparator.reverseOrder()));
    }
}
