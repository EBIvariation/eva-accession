package uk.ac.ebi.eva.accession.core.service.nonhuman;

import uk.ac.ebi.ampt2d.commons.accession.core.models.IEvent;

import uk.ac.ebi.eva.accession.core.contigalias.ContigAliasService;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpClusteredVariantInactiveService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.ClusteredVariantInactiveService;
import uk.ac.ebi.eva.commons.core.models.contigalias.ContigNamingConvention;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ClusteredVariantOperationService {

    private final DbsnpClusteredVariantInactiveService dbsnpClusteredVariantInactiveService;

    private final ClusteredVariantInactiveService clusteredVariantInactiveService;

    private final ContigAliasService contigAliasService;

    public ClusteredVariantOperationService(DbsnpClusteredVariantInactiveService dbsnpClusteredVariantInactiveService,
                                            ClusteredVariantInactiveService clusteredVariantInactiveService,
                                            ContigAliasService contigAliasService) {
        this.dbsnpClusteredVariantInactiveService = dbsnpClusteredVariantInactiveService;
        this.clusteredVariantInactiveService = clusteredVariantInactiveService;
        this.contigAliasService = contigAliasService;
    }

    public List<IEvent<? extends IClusteredVariant, Long>> getAllOperations(Long accession) {
        return getAllOperations(accession, ContigNamingConvention.INSDC);
    }

    public List<IEvent<? extends IClusteredVariant, Long>> getAllOperations(
            Long accession, ContigNamingConvention contigNamingConvention) {
        List<IEvent<? extends IClusteredVariant, Long>> totalOperations = new ArrayList<>();
        totalOperations.addAll(contigAliasService.getEventsWithTranslatedContig(
                dbsnpClusteredVariantInactiveService.getAllEventsInvolvedIn(accession), contigNamingConvention));
        totalOperations.addAll(contigAliasService.getEventsWithTranslatedContig(
                clusteredVariantInactiveService.getAllEventsInvolvedIn(accession), contigNamingConvention));
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
