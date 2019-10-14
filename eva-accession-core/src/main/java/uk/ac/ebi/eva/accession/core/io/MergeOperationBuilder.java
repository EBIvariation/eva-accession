package uk.ac.ebi.eva.accession.core.io;

import org.springframework.data.mongodb.BulkOperationException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;
import uk.ac.ebi.ampt2d.commons.accession.persistence.repositories.IAccessionedObjectRepository;
import uk.ac.ebi.ampt2d.commons.accession.persistence.repositories.IHistoryRepository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static uk.ac.ebi.eva.accession.core.utils.BulkOperationExceptionUtils.extractUniqueHashesForDuplicateKeyError;

public class MergeOperationBuilder<ENTITY extends AccessionedDocument<?, Long>,
        OPERATION_ENTITY extends EventDocument<?, Long, ?>> {

    private IHistoryRepository<Long, OPERATION_ENTITY, String> operationRepository;

    private Function<String, ENTITY> findOneVariantEntityById;

    private BiFunction<ENTITY, ENTITY, OPERATION_ENTITY> mergeOperationFactory;

    public MergeOperationBuilder(IHistoryRepository<Long, OPERATION_ENTITY, String> operationRepository,
                                 IAccessionedObjectRepository<ENTITY, Long> variantRepository,
                                 BiFunction<ENTITY, ENTITY, OPERATION_ENTITY> mergeOperationFactory) {
        this(operationRepository, variantRepository::findOne, mergeOperationFactory);
    }

    public MergeOperationBuilder(IHistoryRepository<Long, OPERATION_ENTITY, String> operationRepository,
                          Function<String, ENTITY> findOneVariantEntityById,
                          BiFunction<ENTITY, ENTITY, OPERATION_ENTITY> mergeOperationFactory) {
        this.operationRepository = operationRepository;
        this.findOneVariantEntityById = findOneVariantEntityById;
        this.mergeOperationFactory = mergeOperationFactory;
    }

    public List<OPERATION_ENTITY> buildMergeOperationsFromException(List<ENTITY> variants,
                                                             BulkOperationException exception) {
        List<OPERATION_ENTITY> operations = new ArrayList<>();
        checkForNulls(variants);
        extractUniqueHashesForDuplicateKeyError(exception)
                .forEach(hash -> {
                    ENTITY mergedInto = findOneVariantEntityById.apply(hash);
                    if (mergedInto == null) {
                        throwMongoConsistencyException(variants, hash);
                    }
                    List<OPERATION_ENTITY> merges = buildMergeOperations(variants,
                                                                         hash,
                                                                         mergedInto);

                    operations.addAll(merges);
                });
        return operations;
    }

    private void throwMongoConsistencyException(List<ENTITY> variants, String hash) {
        String printedVariants = variants
                .stream()
                .filter(v -> v.getHashedMessage().equals(hash))
                .map(v -> v.getClass().toString() + v.getModel().toString())
                .collect(Collectors.toList())
                .toString();
        throw new IllegalStateException(
                "A duplicate key exception was raised with hash " + hash + ", but no document " +
                        "with that hash was found. Make sure you are using ReadPreference=primaryPreferred and "
                        + "WriteConcern=Majority. These variants have that hash: " +
                        printedVariants);
    }

    private List<OPERATION_ENTITY> buildMergeOperations(List<ENTITY> variants, String hash, ENTITY mergedInto) {
        Collection<ENTITY> entities = removeDuplicatesWithSameHashAndAccession(variants.stream());
        checkForNulls(entities);
        return entities
                .stream()
                .filter(v -> v.getHashedMessage().equals(hash)
                        && !v.getAccession().equals(mergedInto.getAccession())
                        && !isAlreadyMergedInto(v, mergedInto))
                .map(origin -> mergeOperationFactory.apply(origin, mergedInto))
                .collect(Collectors.toList());
    }

    private void checkForNulls(Collection<ENTITY> entities) {
        int nullCount = 0;
        for (ENTITY entity : entities) {
            if (entity == null) {
                nullCount++;
            }
        }
        if (nullCount > 0) {
            throw new IllegalStateException(
                    "Could not complete writing merge operations, as " + nullCount + " variants were actually " +
                            "null");
        }
    }

    private <T extends AccessionedDocument> Collection<T> removeDuplicatesWithSameHashAndAccession(
            Stream<T> accessionedEntities) {
        return accessionedEntities.collect(Collectors.toMap(v -> v.getHashedMessage() + v.getAccession().toString(),
                                                            v -> v,
                                                            (a, b) -> a))
                                  .values();
    }

    private boolean isAlreadyMergedInto(ENTITY original, ENTITY mergedInto) {
        List<OPERATION_ENTITY> merges = operationRepository.findAllByAccession(original.getAccession());
        return merges.stream().anyMatch(
                operation ->
                        operation.getEventType().equals(EventType.MERGED)
                                && mergedInto.getAccession().equals(operation.getMergedInto())
                                && original.getHashedMessage().equals(
                                        operation.getInactiveObjects().get(0).getHashedMessage()));
    }
}
