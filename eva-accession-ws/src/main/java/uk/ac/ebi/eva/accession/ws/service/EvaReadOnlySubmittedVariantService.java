package uk.ac.ebi.eva.accession.ws.service;

import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.SubmittedVariantAccessioningDatabaseService;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class EvaReadOnlySubmittedVariantService {

    private final SubmittedVariantAccessioningDatabaseService dbService;

    private final Function<ISubmittedVariant, String> hashingFunction;

    public EvaReadOnlySubmittedVariantService(
            SubmittedVariantAccessioningDatabaseService dbService,
            Function<ISubmittedVariant, String> summaryFunction,
            Function<String, String> hashingFunction
    ) {
        this.dbService = dbService;
        this.hashingFunction = summaryFunction.andThen(hashingFunction);
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> get(List<? extends ISubmittedVariant> accessionedObjects) {
        return dbService.findAllByHash(getHashes(accessionedObjects));
    }

    private List<String> getHashes(List<? extends ISubmittedVariant> accessionObjects) {
        return accessionObjects.stream().map(hashingFunction).collect(Collectors.toList());
    }

    public AccessionWrapper<ISubmittedVariant, String, Long> getByAccession(Long accession)
            throws AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        return dbService.findLastVersionByAccession(accession);
    }

    public AccessionWrapper<ISubmittedVariant, String, Long> getByAccessionAndVersion(Long accession, int version)
            throws AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        return dbService.findByAccessionVersion(accession, version);
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> getByClusteredVariantAccessionIn(
            List<Long> clusteredVariantAccessions) {
        return dbService.findByClusteredVariantAccessionIn(clusteredVariantAccessions);
    }

    public AccessionWrapper<ISubmittedVariant, String, Long> getLastInactive(Long accession) {
        return dbService.getLastInactive(accession);
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> getAllByAccession(Long accession)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        return dbService.getAllByAccession(accession);
    }

}
