package uk.ac.ebi.eva.accession.ws.service;

import uk.ac.ebi.ampt2d.commons.accession.core.DatabaseService;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.service.human.dbsnp.HumanDbsnpClusteredVariantAccessioningDatabaseService;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DbsnpReadOnlyHumanClusteredVariantService {

    private final DatabaseService<IClusteredVariant, String, Long> dbService;

    private final Function<IClusteredVariant, String> hashingFunction;

    public DbsnpReadOnlyHumanClusteredVariantService(
            HumanDbsnpClusteredVariantAccessioningDatabaseService dbService,
            Function<IClusteredVariant, String> summaryFunction,
            Function<String, String> hashingFunction) {
        this.dbService = dbService;
        this.hashingFunction = summaryFunction.andThen(hashingFunction);
    }

    public List<AccessionWrapper<IClusteredVariant, String, Long>> get(List<? extends IClusteredVariant> accessionedObjects) {
        return this.dbService.findAllByHash(this.getHashes(accessionedObjects));
    }

    private List<String> getHashes(List<? extends IClusteredVariant> accessionObjects) {
        return accessionObjects.stream().map(this.hashingFunction).collect(Collectors.toList());
    }

    public AccessionWrapper<IClusteredVariant, String, Long> getByAccession(Long accession) throws AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        return this.dbService.findLastVersionByAccession(accession);
    }

}
