package uk.ac.ebi.eva.accession.ws.service.eva;

import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.ClusteredVariantAccessioningDatabaseService;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Read-only version of {@link uk.ac.ebi.eva.accession.core.service.nonhuman.eva.ClusteredVariantMonotonicAccessioningService}
 */
public class EvaReadOnlyClusteredVariantService {

    private final ClusteredVariantAccessioningDatabaseService dbService;

    private final Function<IClusteredVariant, String> hashingFunction;

    public EvaReadOnlyClusteredVariantService(
            ClusteredVariantAccessioningDatabaseService dbService,
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

    public AccessionWrapper<IClusteredVariant, String, Long> getByAccessionAndVersion(Long accession, int version) throws AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        return this.dbService.findByAccessionVersion(accession, version);
    }

    public List<AccessionWrapper<IClusteredVariant, String, Long>> getByHash(List<String> hashes) {
        return dbService.findAllByHash(hashes);
    }

    public List<AccessionWrapper<IClusteredVariant, String, Long>> getAllByAccession(Long accession) throws AccessionMergedException,
            AccessionDoesNotExistException, AccessionDeprecatedException {
        return dbService.getAllByAccession(accession);
    }

    public AccessionWrapper<IClusteredVariant, String, Long> getLastInactive(Long accession) {
        return dbService.getLastInactive(accession);
    }
}
