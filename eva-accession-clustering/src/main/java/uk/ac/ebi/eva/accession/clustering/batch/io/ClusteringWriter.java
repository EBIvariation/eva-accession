/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.accession.clustering.batch.io;

import com.mongodb.MongoBulkWriteException;
import htsjdk.samtools.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.Assert;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.core.models.GetOrCreateAccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;

import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringCounts;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantClassifier;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static org.springframework.data.mongodb.core.query.Update.update;
import static uk.ac.ebi.eva.accession.clustering.batch.io.ClusteredVariantMergingPolicy.Priority;
import static uk.ac.ebi.eva.accession.clustering.batch.io.ClusteredVariantMergingPolicy.prioritise;

/**
 * This writer has two parts:
 * 1. Use the accessioning service to generate new RS IDs or get existing ones
 * 2. Update the submitted variants to include the "rs" field with the generated/retrieved accessions
 *
 * Some edge cases take into account if a clustered variant is multimap. The definition of multimap variants that this
 * class uses is "clustered variants whose mapWeight is 2 or greater". Another definition is "clustered variants
 * whose accession maps several times in the same assembly". Although both definitions should yield the same
 * set of variants, the check for the second definition is less efficient and less accurate: both the active and the
 * deprecated/merged collections should be queried, and we lost clusteredVariantOperations during the
 * deprecation pipeline in the dbSNP import due to a bug.
 */
public class ClusteringWriter implements ItemWriter<SubmittedVariantEntity> {
    private static final Logger logger = LoggerFactory.getLogger(ClusteringWriter.class);

    public static final String ACCESSION_KEY = "accession";

    public static final String RS_KEY = "rs";

    public static final String ID = "_id";

    private MongoTemplate mongoTemplate;

    private ClusteredVariantAccessioningService clusteredService;

    private Function<IClusteredVariant, String> clusteredHashingFunction;

    private Map<String, Long> assignedAccessions;

    private Long accessioningMonotonicInitSs;

    private Long accessioningMonotonicInitRs;

    private ClusteringCounts clusteringCounts;

    private boolean processClusteredRemappedVariants;

    public ClusteringWriter(MongoTemplate mongoTemplate,
                            ClusteredVariantAccessioningService clusteredVariantAccessioningService,
                            Long accessioningMonotonicInitSs,
                            Long accessioningMonotonicInitRs,
                            ClusteringCounts clusteringCounts,
                            boolean processClusteredRemappedVariants) {
        this.mongoTemplate = mongoTemplate;
        this.clusteredService = clusteredVariantAccessioningService;
        this.clusteredHashingFunction = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        this.assignedAccessions = new HashMap<>();
        Assert.notNull(accessioningMonotonicInitSs, "accessioningMonotonicInitSs must not be null. Check autowiring.");
        this.accessioningMonotonicInitSs = accessioningMonotonicInitSs;
        this.accessioningMonotonicInitRs = accessioningMonotonicInitRs;
        this.clusteringCounts = clusteringCounts;
        this.processClusteredRemappedVariants = processClusteredRemappedVariants;
    }

    @Override
    public void write(List<? extends SubmittedVariantEntity> submittedVariantEntities)
            throws MongoBulkWriteException, AccessionCouldNotBeGeneratedException {
        assignedAccessions.clear();

        // Write new Clustered Variants in mongo and get existing ones. May merge clustered variants
        getOrCreateClusteredVariantAccessions(submittedVariantEntities);

        // Update submitted variants "rs" field
        clusterSubmittedVariants(submittedVariantEntities);
    }

    private void getOrCreateClusteredVariantAccessions(List<? extends SubmittedVariantEntity> submittedVariantEntities)
            throws AccessionCouldNotBeGeneratedException {
        if (processClusteredRemappedVariants) {
            List<SubmittedVariantEntity> processedClusteredVariants = processClusteredRemappedVariantsRSSplit(submittedVariantEntities);
            clusteringCounts.addClusteredVariantsRSSplit(processedClusteredVariants.size());
            submittedVariantEntities = Collections.unmodifiableList(submittedVariantEntities.stream()
                                                                            .filter(sve->!processedClusteredVariants.contains(sve))
                                                                            .collect(Collectors.toList()));
        }
        List<ClusteredVariant> clusteredVariants = submittedVariantEntities.stream()
                                                                           .map(this::toClusteredVariant)
                                                                           .collect(Collectors.toList());
        if (!clusteredVariants.isEmpty()) {
            List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> accessionWrappers =
                    clusteredService.getOrCreate(clusteredVariants);

            List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> accessionNoMultimap =
                    excludeMultimaps(accessionWrappers);

            accessionNoMultimap.forEach(x -> assignedAccessions.put(x.getHash(), x.getAccession()));

            long newAccessions = accessionWrappers.stream().filter(GetOrCreateAccessionWrapper::isNewAccession).count();
            clusteringCounts.addClusteredVariantsCreated(newAccessions);
        }
        checkForMerges(submittedVariantEntities);
    }

    /**
     * This method handles a specific scenario in clustering where an already clustered variant is remapped but the
     * remapped variant is at a different location or a different variant type.
     * <p>
     * SubmittedVariantEntity                                                           ClusteredVariantEntity
     * SS	RS	ASM	    STUDY	CONTIG	POS	    REF	ALT                    RS	HASH                ASM	    POS	    CONTIG	TYPE
     * 501	306	ASM1	PRJEB1	Chr1	1000		TT  (org)              306  ASM1_Chr1_1000_INS	ASM1	1000	Chr1	INS
     * 500	306	ASM1	PRJEB1	Chr1	1000	A	AT  (org)
     * 505	306	ASM2	PRJEB1	Chr1	1500		TT  (remapped)
     * 504	306	ASM2	PRJEB1	Chr1	1000		T   (remapped)
     * <p>
     * SS id 504 and 505 has same RS because of remapping, but they are now at different positions and can't have
     * same RS. RS split needs to be done here.
     * <p>
     * SubmittedVariantEntity                                                           ClusteredVariantEntity
     * SS	RS	ASM	    STUDY	CONTIG	POS	    REF	ALT                    RS	HASH                ASM	    POS	    CONTIG	TYPE
     * 501	306	ASM1	PRJEB1	Chr1	1000		TT  (org)              306  ASM1_Chr1_1000_INS	ASM1	1000	Chr1	INS
     * 500	306	ASM1	PRJEB1	Chr1	1000	A	AT  (org)              306  ASM2_Chr1_1000_INS	ASM2	1000	Chr1	INS
     * 505	400	ASM2	PRJEB1	Chr1	1500		TT  (remapped)         400  ASM2_Chr1_1500_INS	ASM2	1500	Chr1	INS
     * 504	306	ASM2	PRJEB1	Chr1	1000		T   (remapped)
     */
    private List<SubmittedVariantEntity> processClusteredRemappedVariantsRSSplit(List<? extends SubmittedVariantEntity> submittedVariantEntities)
            throws AccessionCouldNotBeGeneratedException{
        List<SubmittedVariantEntity> clusteredRemappedSubmittedVariantsList = getClusteredAndRemappedVariants(submittedVariantEntities);
        Map<Long,List<SubmittedVariantEntity>> submittedVariantsEligibleForRSSplitList = getVariantsEligibleForRSSplit(clusteredRemappedSubmittedVariantsList);
        if(!submittedVariantsEligibleForRSSplitList.isEmpty()){
            List<SubmittedVariantEntity> processedVariants = processRSSplitVariants(submittedVariantsEligibleForRSSplitList);
            return processedVariants;
        }
        return Collections.emptyList();
    }

    private List<SubmittedVariantEntity> getClusteredAndRemappedVariants(List<? extends SubmittedVariantEntity> submittedVariantList){
        return submittedVariantList.stream()
                .filter(v -> v.getClusteredVariantAccession() != null)
                .filter(v -> !StringUtil.isBlank(v.getRemappedFrom()))
                .collect(Collectors.toList());
    }

    private Map<Long,List<SubmittedVariantEntity>> getVariantsEligibleForRSSplit(List<? extends SubmittedVariantEntity> submittedVariantList) {
        List<IClusteredVariant> clusteredVariantList = submittedVariantList.stream()
                .map(this::toClusteredVariantEntity)
                .collect(Collectors.toList());
        List<String> clusteredVariantsRSExistsInDB = clusteredService.get(clusteredVariantList).stream()
                .map(AccessionWrapper::getHash)
                .collect(Collectors.toList());
        Map<Long,List<SubmittedVariantEntity>> submittedVariantRSSplitMap = submittedVariantList.stream()
                .filter(sve -> !clusteredVariantsRSExistsInDB.contains(toClusteredVariantEntity(sve).getHashedMessage()))
                .collect(Collectors.groupingBy(sve->sve.getClusteredVariantAccession()));

            return submittedVariantRSSplitMap;
    }

    private List<SubmittedVariantEntity> processRSSplitVariants(Map<Long,List<SubmittedVariantEntity>> submittedVariantRSSplitMap)
            throws AccessionCouldNotBeGeneratedException{
        String assembly = submittedVariantRSSplitMap.values().iterator().next().get(0).getReferenceSequenceAccession();
        List<SubmittedVariantEntity> submittedVariantRetainingRsList = getSubmittedVariantsRetainingRS(submittedVariantRSSplitMap, assembly);
        List<SubmittedVariantEntity> submittedVariantGeneratingNewRSList = submittedVariantRSSplitMap.values().stream()
                .flatMap(sveList->sveList.stream())
                .filter(sve->!submittedVariantRetainingRsList.contains(sve))
                .collect(Collectors.toList());
        List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> accessionWrappers =
                generateNewRSIdAndGetAccessionWrappers(submittedVariantGeneratingNewRSList);
        List<ClusteredVariantOperationEntity> clusteredVariantOperationEntityList =
                getClusteredVariantOperations(accessionWrappers, submittedVariantGeneratingNewRSList);
        List<ClusteredVariantEntity> clusteredVariantEntityList = submittedVariantRetainingRsList.stream()
                .map(this::toClusteredVariantEntity)
                .collect(Collectors.toList());

        mongoTemplate.insert(clusteredVariantEntityList, ClusteredVariantEntity.class);
        mongoTemplate.insert(clusteredVariantOperationEntityList,ClusteredVariantOperationEntity.class);

        Map<String, Long> newlyGeneratedAccessions = accessionWrappers.stream()
                                                                      .collect(Collectors.toMap(ac->ac.getHash(),ac->ac.getAccession()));
        for(SubmittedVariantEntity submittedVariantEntity: submittedVariantGeneratingNewRSList) {
            Priority prioritised = new Priority(newlyGeneratedAccessions.get(toClusteredVariantEntity(submittedVariantEntity).getHashedMessage()),
                                                submittedVariantEntity.getClusteredVariantAccession());
            updateSubmittedVariantsWithNewlyGeneratedRS(submittedVariantEntity, prioritised, SubmittedVariantEntity.class,
                                                        SubmittedVariantOperationEntity.class);
            updateSubmittedVariantsWithNewlyGeneratedRS(submittedVariantEntity, prioritised, DbsnpSubmittedVariantEntity.class,
                                                        DbsnpSubmittedVariantOperationEntity.class);
        }

        return submittedVariantRSSplitMap.values().stream()
                .flatMap(sveList->sveList.stream())
                .collect(Collectors.toList());
    }

    private void updateSubmittedVariantsWithNewlyGeneratedRS(SubmittedVariantEntity submittedVariantEntity,
                                                             Priority prioritised, Class<? extends SubmittedVariantEntity> submittedVariantCollection,
                                                             Class<? extends EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>
                                                                     submittedOperationCollection){
        Query querySubmitted = query(where(ID).is(submittedVariantEntity.getHashedMessage()));
        List<? extends SubmittedVariantEntity> svToUpdate =
                    mongoTemplate.find(querySubmitted, submittedVariantCollection);

        Update update = new Update();
        update.set(RS_KEY, prioritised.accessionToKeep);
        mongoTemplate.upsert(querySubmitted, update, submittedVariantCollection);
        clusteringCounts.addSubmittedVariantsUpdatedRs(svToUpdate.size());

        if (!svToUpdate.isEmpty()) {
            List<SubmittedVariantOperationEntity> operations =
                    svToUpdate.stream()
                              .map(sv -> buildSubmittedOperation(sv, prioritised.accessionToKeep))
                              .collect(Collectors.toList());
            mongoTemplate.insert(operations, submittedOperationCollection);
            clusteringCounts.addSubmittedVariantsUpdateOperationWritten(operations.size());
        }
    }


    private List<SubmittedVariantEntity> getSubmittedVariantsRetainingRS(Map<Long,List<SubmittedVariantEntity>> submittedVariantsRSSplitMap, String assembly){
        List<SubmittedVariantEntity> submittedVariantRetainingRsList = new ArrayList<>();
        for(Map.Entry<Long,List<SubmittedVariantEntity>> entry:submittedVariantsRSSplitMap.entrySet()){
            try {
                boolean isThereAnEntryForRSAndAssembly = clusteredService.getAllByAccession(entry.getKey()).stream()
                        .map(accessionWrapper -> accessionWrapper.getData().getAssemblyAccession())
                        .anyMatch(assemblyAccession -> assemblyAccession.equals(assembly));

                if(!isThereAnEntryForRSAndAssembly){
                    submittedVariantRetainingRsList.add(entry.getValue().get(0));
                }
            }catch (AccessionMergedException| AccessionDoesNotExistException| AccessionDeprecatedException ex){
                logger.error("exception occurred while getting variants with rs ID {" + entry.getKey() + "}. " + ex);
            }
        }
        return submittedVariantRetainingRsList;
    }

    private List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> generateNewRSIdAndGetAccessionWrappers(
            List<SubmittedVariantEntity> submittedVariantNewRSList) throws AccessionCouldNotBeGeneratedException{
        List<ClusteredVariantEntity> clusteredVariantNewRSList = submittedVariantNewRSList.stream()
                .map(this::toClusteredVariantEntity)
                .collect(Collectors.toList());
        List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> accessionWrappers =
                clusteredService.getOrCreate(clusteredVariantNewRSList);
        long newAccessions = accessionWrappers.stream().filter(GetOrCreateAccessionWrapper::isNewAccession).count();
        clusteringCounts.addClusteredVariantsCreated(newAccessions);

        return accessionWrappers;
    }

    private List<ClusteredVariantOperationEntity> getClusteredVariantOperations(List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> accessionWrappers,
                                                                                List<SubmittedVariantEntity> submittedVariantEntityListForRS){
        List<ClusteredVariantOperationEntity> clusteredVariantOperationEntityList = new ArrayList<>();
        for(GetOrCreateAccessionWrapper<IClusteredVariant,String,Long> accessionWrapper : accessionWrappers) {
            SubmittedVariantEntity submittedVariantEntity = submittedVariantEntityListForRS.stream()
                    .filter(sve->toClusteredVariantEntity(sve).getHashedMessage().equals(accessionWrapper.getHash()))
                    .findFirst().get();
            ClusteredVariantOperationEntity clusteredVariantOperationEntity = new ClusteredVariantOperationEntity();
            //TODO : create a new EventType and replace this
            clusteredVariantOperationEntity.fill(EventType.MERGED,
                                                 submittedVariantEntity.getClusteredVariantAccession(),
                                                 accessionWrapper.getAccession(),
                                                 new StringBuilder().append("Due to RS Split new accession id ")
                                                                    .append(accessionWrapper.getAccession())
                                                                    .append(" created for remapped accession ")
                                                                    .append(submittedVariantEntity.getClusteredVariantAccession())
                                                                    .toString(),
                                                 null);
            clusteredVariantOperationEntityList.add(clusteredVariantOperationEntity);
        }

        return clusteredVariantOperationEntityList;
    }

    private ClusteredVariantEntity toClusteredVariantEntity(SubmittedVariantEntity submittedVariantEntity) {
        return new ClusteredVariantEntity(submittedVariantEntity.getClusteredVariantAccession(), getClusteredVariantHash(submittedVariantEntity),
                toClusteredVariant(submittedVariantEntity));
    }

    private ClusteredVariant toClusteredVariant(SubmittedVariantEntity submittedVariantEntity) {
        ClusteredVariant clusteredVariant = new ClusteredVariant(submittedVariantEntity.getReferenceSequenceAccession(),
                                                                 submittedVariantEntity.getTaxonomyAccession(),
                                                                 submittedVariantEntity.getContig(),
                                                                 submittedVariantEntity.getStart(),
                                                                 getVariantType(
                                                                         submittedVariantEntity.getReferenceAllele(),
                                                                         submittedVariantEntity.getAlternateAllele()),
                                                                 submittedVariantEntity.isValidated(),
                                                                 submittedVariantEntity.getCreatedDate());
        return clusteredVariant;
    }

    private VariantType getVariantType(String reference, String alternate) {
        VariantType variantType = VariantClassifier.getVariantClassification(reference, alternate);
        return variantType;
    }

    private void checkForMerges(List<? extends SubmittedVariantEntity> submittedVariantEntities) {
        for (SubmittedVariantEntity submittedVariant : submittedVariantEntities) {
            if (submittedVariant.getClusteredVariantAccession() != null && submittedVariant.getRemappedFrom() != null) {
                String hash = clusteredHashingFunction.apply(toClusteredVariant(submittedVariant));
                Long accessionInDatabase = assignedAccessions.get(hash);
                //accessionInDatabase will be null if it was excluded for being a multimap
                if (accessionInDatabase != null &&
                        !submittedVariant.getClusteredVariantAccession().equals(accessionInDatabase)) {
                    merge(submittedVariant.getClusteredVariantAccession(), hash, accessionInDatabase);
                }
            }
        }
    }

    private void merge(Long providedAccession, String hash, Long accessionInDatabase) {
        Priority prioritised = prioritise(providedAccession, accessionInDatabase);

        Query queryClustered = query(where(ACCESSION_KEY).is(prioritised.accessionToBeMerged));
        List<? extends ClusteredVariantEntity> clusteredVariantToMerge =
                mongoTemplate.find(queryClustered, getClusteredVariantCollection(prioritised.accessionToBeMerged));

        List<? extends ClusteredVariantEntity> clusteredVariantToKeep =
                mongoTemplate.find(query(where(ACCESSION_KEY).is(prioritised.accessionToKeep)),
                                   getClusteredVariantCollection(prioritised.accessionToKeep));

        if (isMultimap(clusteredVariantToMerge) || isMultimap(clusteredVariantToKeep)) {
            // multimap! don't merge. see isMultimap() below for more details
            return;
        }

        assignedAccessions.put(hash, prioritised.accessionToKeep);

        // write operations for clustered variant being merged
        List<ClusteredVariantOperationEntity> operations =
                clusteredVariantToMerge.stream()
                                       .map(c -> buildClusteredOperation(c, prioritised.accessionToKeep))
                                       .collect(Collectors.toList());
        mongoTemplate.insert(operations, getClusteredOperationCollection(prioritised.accessionToBeMerged));
        clusteringCounts.addClusteredVariantsMergeOperationsWritten(clusteredVariantToMerge.size());

        mongoTemplate.updateMulti(queryClustered, update(ACCESSION_KEY, prioritised.accessionToKeep),
                                  getClusteredVariantCollection(prioritised.accessionToBeMerged));
        clusteringCounts.addClusteredVariantsUpdated(clusteredVariantToMerge.size());

        // Update submitted variants linked to the clustered variant we just merged.
        // This has to happen for both EVA and dbsnp SS because previous cross merges might have happened.
        updateSubmittedVariants(prioritised, SubmittedVariantEntity.class, SubmittedVariantOperationEntity.class);
        updateSubmittedVariants(prioritised, DbsnpSubmittedVariantEntity.class,
                                DbsnpSubmittedVariantOperationEntity.class);
    }

    private Class<? extends ClusteredVariantEntity> getClusteredVariantCollection(Long accession) {
        return isEvaClusteredAccession(accession) ? ClusteredVariantEntity.class : DbsnpClusteredVariantEntity.class;
    }

    private boolean isEvaClusteredAccession(Long accession) {
        return accession >= accessioningMonotonicInitRs;
    }

    /**
     * In EVA-2003 we decided not to merge any RS ID that maps to several places in the same assembly
     * (mapping weight > 1) as this might be a signal of a low quality variant, and merging other "real" variants
     * in the same RS would just make things more complicated.
     *
     * Note that for submitted variants the test is not this simple, as 1:1000:A:T and 1:1000:A:G can be present in the
     * same assembly and still not classify as multimap.
     */
    private boolean isMultimap(List<? extends IClusteredVariant> clusteredVariants) {
        return clusteredVariants.stream().anyMatch(cv -> cv.getMapWeight() != null && cv.getMapWeight() > 1);
    }

    private boolean isMultimap(IClusteredVariant clusteredVariant) {
        return isMultimap(Collections.singletonList(clusteredVariant));
    }

    private Class<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>
    getClusteredOperationCollection(Long accession) {
        return isEvaClusteredAccession(accession) ?
                ClusteredVariantOperationEntity.class : DbsnpClusteredVariantOperationEntity.class;
    }

    private ClusteredVariantOperationEntity buildClusteredOperation(ClusteredVariantEntity originalClusteredVariant,
                                                                    Long clusteredVariantMergedInto) {
        ClusteredVariantInactiveEntity inactiveEntity = new ClusteredVariantInactiveEntity(originalClusteredVariant);

        Long originalAccession = originalClusteredVariant.getAccession();
        String reason = "Original rs" + originalAccession + " was merged into rs" + clusteredVariantMergedInto + ".";

        ClusteredVariantOperationEntity operation = new ClusteredVariantOperationEntity();
        operation.fill(EventType.MERGED, originalAccession, clusteredVariantMergedInto, reason,
                       Collections.singletonList(inactiveEntity));
        return operation;
    }

    /**
     * This function updates the clustered variant accession (rs) of submitted variants when the rs makes a
     * collision with another rs and they have to be merged.
     */
    private void updateSubmittedVariants(
            Priority prioritised,
            Class<? extends SubmittedVariantEntity> submittedVariantCollection,
            Class<? extends EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>
                    submittedOperationCollection) {
        Query querySubmitted = query(where(RS_KEY).is(prioritised.accessionToBeMerged));
        List<? extends SubmittedVariantEntity> svToUpdate =
                mongoTemplate.find(querySubmitted, submittedVariantCollection);

        Update update = new Update();
        update.set(RS_KEY, prioritised.accessionToKeep);
        mongoTemplate.updateMulti(querySubmitted, update, submittedVariantCollection);
        clusteringCounts.addSubmittedVariantsUpdatedRs(svToUpdate.size());

        if (!svToUpdate.isEmpty()) {
            List<SubmittedVariantOperationEntity> operations =
                    svToUpdate.stream()
                              .map(sv -> buildSubmittedOperation(sv, prioritised.accessionToKeep))
                              .collect(Collectors.toList());
            mongoTemplate.insert(operations, submittedOperationCollection);
            clusteringCounts.addSubmittedVariantsUpdateOperationWritten(operations.size());
        }
    }

    private SubmittedVariantOperationEntity buildSubmittedOperation(SubmittedVariantEntity originalSubmittedVariant,
                                                                    Long clusteredVariantMergedInto) {
        SubmittedVariantInactiveEntity inactiveEntity = new SubmittedVariantInactiveEntity(originalSubmittedVariant);

        Long originalClusteredVariant = originalSubmittedVariant.getClusteredVariantAccession();
        String reason = "Original rs" + originalClusteredVariant + " was merged into rs" + clusteredVariantMergedInto + ".";

        Long accession = originalSubmittedVariant.getAccession();
        SubmittedVariantOperationEntity operation = new SubmittedVariantOperationEntity();

        // Note the next null in accessionIdDestiny. We are not merging the submitted variant into
        // anything. We are updating the submitted variant, changing its rs field
        operation.fill(EventType.UPDATED, accession, null, reason, Collections.singletonList(inactiveEntity));
        return operation;
    }

    /**
     * From EVA-2071, do not cluster submitted variants into a multimap clustered variant.
     *
     * This function removes candidate clustered variant accessions if they are multimap. This means that some submitted
     * variants will be kept unclustered. This potentially will be revisited in the future, but for now (release 2) we
     * are leaving this out of scope.
     */
    private List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> excludeMultimaps(
            List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> accessionWrappers) {
        return accessionWrappers.stream().filter(x -> !isMultimap(x.getData())).collect(Collectors.toList());
    }

    /**
     * This function assigns a clustered variant accession (rs) to the submitted variants that didn't have any.
     */
    private void clusterSubmittedVariants(List<? extends SubmittedVariantEntity> submittedVariantEntities) {
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                              SubmittedVariantEntity.class);
        BulkOperations dbsnpBulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                                   DbsnpSubmittedVariantEntity.class);
        BulkOperations bulkHistoryOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                SubmittedVariantOperationEntity.class);
        BulkOperations dbsnpBulkHistoryOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                DbsnpSubmittedVariantOperationEntity.class);
        long numUpdates = 0;
        long numDbsnpUpdates = 0;
        for (SubmittedVariantEntity submittedVariantEntity : submittedVariantEntities) {
            if (submittedVariantEntity.getClusteredVariantAccession() != null) {
                // no need to update the rs of a submitted variant that already has the correct rs
                continue;
            }

            Long rsid = getClusteredVariantAccession(submittedVariantEntity);
            if (rsid == null) {
                // no candidate for clustering. e.g. the candidate is a multimap clustered variant (EVA-2071)
                clusteringCounts.addSubmittedVariantsKeptUnclustered(1);
                continue;
            }
            // Query to update the RSid in submittedVariantEntity
            Query updateRsQuery = query(where("_id").is(submittedVariantEntity.getId()));
            Update updateRsUpdate = new Update();
            updateRsUpdate.set(RS_KEY, rsid);

            // Query to create the update operation history
            SubmittedVariantOperationEntity updateOperation = new SubmittedVariantOperationEntity();
            SubmittedVariantInactiveEntity inactiveEntity = new SubmittedVariantInactiveEntity(submittedVariantEntity);
            updateOperation.fill(
                    EventType.UPDATED,
                    submittedVariantEntity.getAccession(),
                    null,
                    "Clustering submitted variant " + submittedVariantEntity.getAccession() + " with rs" + rsid,
                    Collections.singletonList(inactiveEntity)
            );

            if (isEvaSubmittedVariant(submittedVariantEntity)) {
                bulkOperations.updateOne(updateRsQuery, updateRsUpdate);
                bulkHistoryOperations.insert(updateOperation);
                ++numUpdates;
            } else {
                dbsnpBulkOperations.updateOne(updateRsQuery, updateRsUpdate);
                dbsnpBulkHistoryOperations.insert(updateOperation);
                ++numDbsnpUpdates;
            }
        }
        if (numUpdates > 0) {
            bulkOperations.execute();
            clusteringCounts.addSubmittedVariantsClustered(numUpdates);
            bulkHistoryOperations.execute();
            clusteringCounts.addSubmittedVariantsUpdateOperationWritten(numUpdates);
        }
        if (numDbsnpUpdates > 0) {
            dbsnpBulkOperations.execute();
            clusteringCounts.addSubmittedVariantsClustered(numDbsnpUpdates);
            dbsnpBulkHistoryOperations.execute();
            clusteringCounts.addSubmittedVariantsUpdateOperationWritten(numDbsnpUpdates);
        }
    }

    private boolean isEvaSubmittedVariant(SubmittedVariantEntity submittedVariant) {
        return submittedVariant.getAccession() >= accessioningMonotonicInitSs;
    }

    private Long getClusteredVariantAccession(SubmittedVariantEntity submittedVariantEntity) {
        String hash = getClusteredVariantHash(submittedVariantEntity);
        return assignedAccessions.get(hash);
    }

    private String getClusteredVariantHash(SubmittedVariantEntity submittedVariantEntity) {
        ClusteredVariant clusteredVariant = toClusteredVariant(submittedVariantEntity);
        String hash = clusteredHashingFunction.apply(clusteredVariant);
        return hash;
    }
}
