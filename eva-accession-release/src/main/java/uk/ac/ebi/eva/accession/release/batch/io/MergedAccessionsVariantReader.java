package uk.ac.ebi.eva.accession.release.batch.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.release.parameters.ReportPathResolver;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.models.VariantTypeToSOAccessionMap;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * Read all ClusteredVariant Accessions from file in batches
 */
public class MergedAccessionsVariantReader implements ItemStreamReader<List<Variant>> {
    private static final Logger logger = LoggerFactory.getLogger(MergedAccessionsVariantReader.class);

    public static final String CVE_ACC_FIELD = "accession";
    public static final String CVE_ASM_FIELD = "asm";

    public static final String CVE_OPS_ACCESSION_FIELD = "accession";
    public static final String CVE_OPS_EVENT_TYPE_FIELD = "eventType";
    public static final String CVE_OPS_ASM_FIELD = "inactiveObjects.asm";

    public static final String SVE_OPS_RS_FIELD = "inactiveObjects.rs";
    public static final String SVE_OPS_EVENT_ASM_FIELD = "inactiveObjects.seq";
    public static final String SVE_OPS_EVENT_TAX_FIELD = "inactiveObjects.tax";
    public static final String SVE_OPS_EVENT_TYPE_FIELD = "eventType";

    private static final String MERGE_OPERATION_REASON_FIRST_WORD = "Original";
    private static final String DECLUSTER_OPERATION_REASON_FIRST_WORD = "Declustered: ";
    public static final String MERGED_INTO_KEY = "CURR";

    public static final String VARIANT_CLASS_KEY = "VC";
    public static final String STUDY_ID_KEY = "SID";
    public static final String CLUSTERED_VARIANT_VALIDATED_KEY = "RS_VALIDATED";
    public static final String SUBMITTED_VARIANT_VALIDATED_KEY = "SS_VALIDATED";
    public static final String ASSEMBLY_MATCH_KEY = "ASMM";
    public static final String ALLELES_MATCH_KEY = "ALMM";
    public static final String REMAPPED_KEY = "REMAPPED";
    public static final String SUPPORTED_BY_EVIDENCE_KEY = "LOE";
    private static final String RS_PREFIX = "rs";

    private MongoTemplate mongoTemplate;
    private String rsAccFile;
    private String assembly;
    private int taxonomy;
    private int chunkSize;
    private String outputDir;

    private BufferedReader reader;
    private BufferedWriter writer;

    public MergedAccessionsVariantReader(MongoTemplate mongoTemplate, String rsAccFile, String assembly, int taxonomy,
                                         int chunkSize, String outputDir) {
        this.mongoTemplate = mongoTemplate;
        this.assembly = assembly;
        this.taxonomy = taxonomy;
        this.rsAccFile = rsAccFile;
        this.chunkSize = chunkSize;
        this.outputDir = outputDir;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            reader = new BufferedReader(new FileReader(rsAccFile));
            writer = new BufferedWriter(new FileWriter(ReportPathResolver.getEvaMergedDeprecatedIdsReportPath(outputDir, assembly)
                    .toFile()));
        } catch (IOException e) {
            throw new ItemStreamException("Error opening file: ", e);
        }
    }


    @Override
    public List<Variant> read() {
        List<Long> cveAccList = new ArrayList<>();
        String line;

        try {
            while (cveAccList.size() < chunkSize && (line = reader.readLine()) != null) {
                String rsAcc = line.split("[ \t]+")[0].trim();
                if (!rsAcc.isEmpty()) {
                    cveAccList.add(Long.parseLong(rsAcc));
                }
            }
            if (cveAccList.isEmpty()) {
                return null;
            } else {
                return processCveAccessions(cveAccList);
            }
        } catch (IOException e) {
            throw new ItemStreamException("Error reading variant Accessions from file", e);
        }
    }

    public List<Variant> processCveAccessions(List<Long> cveAccList) {
        Set<Long> cveAccSet = new HashSet<>(cveAccList);

        // Get active accessions
        Set<Long> activeAccessionsSet = getClusteredVariantEntities(cveAccSet).stream()
                .map(ClusteredVariantEntity::getAccession)
                .collect(Collectors.toSet());

        // check and report accessions if an accession is both active and also has merged/deprecated operations
        if (!activeAccessionsSet.isEmpty()) {
            logger.warn("Accession is both Active and has Merged/Deprecated operations: The following accessions are active as well as have merged/deprecated operations: {}", activeAccessionsSet);
            // remove all the accessions which are active currently. These will be released in the current RS file
            cveAccSet.removeAll(activeAccessionsSet);
        }

        Map<Long, Set<EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>> mergedIntoActiveAccessionsMap = new HashMap<>();
        Map<Long, Set<Long>> mergedIntoDeprecatedAccessionsMap = new HashMap<>();
        cveAccSet.forEach(acc -> mergedIntoActiveAccessionsMap.put(acc, new HashSet<>()));
        cveAccSet.forEach(acc -> mergedIntoDeprecatedAccessionsMap.put(acc, new HashSet<>()));

        // Iterates through merge chains to determine if the resulting CVE is active, deprecated, or in an undefined state
        iterateThroughMergeChainAndResolveCVEStatus(cveAccSet, mergedIntoActiveAccessionsMap, mergedIntoDeprecatedAccessionsMap);

        // Compute Results

        // deprecated accessions set - cve accessions which are deprecated and not present in active accessions set
        Set<Long> mergedDeprecatedAccSet = mergedIntoDeprecatedAccessionsMap.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .filter(entry -> mergedIntoActiveAccessionsMap.get(entry.getKey()).isEmpty())
                .map(entry -> entry.getKey())
                .collect(Collectors.toSet());
        if (!mergedDeprecatedAccSet.isEmpty()) {
            writeMergeDeprecatedAccessionsToFile(mergedDeprecatedAccSet);
        }

        // active accessions set - find accession which resolved to active, also log ones which resolves to multiple active ones
        Set<Long> mergedAccSet = new HashSet<>();
        mergedIntoActiveAccessionsMap.forEach((acc, accSet) -> {
            if (!accSet.isEmpty()) {
                mergedAccSet.add(acc);
                if (accSet.size() > 1) {
                    logger.error("Accession {} resolves into multiple active accessions {}", acc, accSet);
                }
            }
        });

        // log accessions which could not be resolved into active or deprecated
        Set<Long> couldNotDetermineSet = cveAccSet.stream()
                .filter(acc -> !mergedAccSet.contains(acc) && !mergedDeprecatedAccSet.contains(acc))
                .collect(Collectors.toSet());
        if (!couldNotDetermineSet.isEmpty()) {
            logger.error("Could not determine the status of following accessions: {}", couldNotDetermineSet);
        }

        Map<Long, List<EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>> sveOpsEntitiesMap = getSVEOpsMap(mergedAccSet);

        List<Variant> variantList = new ArrayList<>();
        for (Long acc : mergedAccSet) {
            List<EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>> sveOpsEntityList = sveOpsEntitiesMap.get(acc);
            if (sveOpsEntityList != null) {
                EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity> cveOp = mergedIntoActiveAccessionsMap.get(acc)
                        .stream().sorted(Comparator.comparing(EventDocument::getMergedInto)).findFirst().get();
                variantList.addAll(getVariants(cveOp, sveOpsEntityList));
            }
        }

        return variantList;
    }

    private void iterateThroughMergeChainAndResolveCVEStatus(Set<Long> cveAccSet,
                                                             Map<Long, Set<EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>> mergedIntoActiveAccessionsMap,
                                                             Map<Long, Set<Long>> mergedIntoDeprecatedAccessionsMap) {
        // Get all Merged and Deprecated CVE Ops for the given CVE accessions
        Map<Long, Map<Boolean, List<EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>>> cveMergedDeprecatedOps = getMergedAndDeprecatedCVEOpsMap(cveAccSet);

        // update mergedInto and deprecated accessions map
        Map<Long, List<EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>> orgAccMergeIntoMap = new HashMap<>();

        for (Map.Entry<Long, Map<Boolean, List<EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>>> entry : cveMergedDeprecatedOps.entrySet()) {
            orgAccMergeIntoMap.put(entry.getKey(), entry.getValue().getOrDefault(Boolean.TRUE, Collections.emptyList()));

            Set<Long> deprecated = entry.getValue().getOrDefault(Boolean.FALSE, Collections.emptyList()).stream()
                    .map(EventDocument::getAccession).collect(Collectors.toSet());
            mergedIntoDeprecatedAccessionsMap.get(entry.getKey()).addAll(deprecated);
        }

        // create a map to keep track of processed accession in the merge chain for each accession
        Map<Long, Set<Long>> alreadyProcessedAccessionsMap = cveAccSet.stream()
                .collect(Collectors.toMap(acc -> acc, acc -> new HashSet<>(Collections.singletonList(acc))));

        // initialize processing set with mergeInto accessions
        Set<Long> processingAccSet = orgAccMergeIntoMap.values().stream()
                .flatMap(Collection::stream)
                .map(EventDocument::getMergedInto)
                .collect(Collectors.toSet());

        while (!processingAccSet.isEmpty()) {
            // Get active accessions
            Set<Long> activeAccSet = getClusteredVariantEntities(processingAccSet).stream()
                    .map(ClusteredVariantEntity::getAccession)
                    .collect(Collectors.toSet());
            orgAccMergeIntoMap.forEach((orgAcc, mergeIntoList) -> {
                mergeIntoList.forEach(eventDoc -> {
                    if (activeAccSet.contains(eventDoc.getMergedInto())) {
                        mergedIntoActiveAccessionsMap.get(orgAcc).add(eventDoc);
                    }
                });
            });

            // update already processed
            Set<Long> tempProcessingAccSet = processingAccSet.stream().collect(Collectors.toSet());
            orgAccMergeIntoMap.forEach((orgAcc, mergeIntoList) -> {
                mergeIntoList.forEach(eventDoc -> {
                    if (tempProcessingAccSet.contains(eventDoc.getMergedInto())) {
                        alreadyProcessedAccessionsMap.get(orgAcc).add(eventDoc.getMergedInto());
                    }
                });
            });

            // Get all Merged and Deprecated CVE Ops for the given CVE accession
            Map<Long, Map<Boolean, List<EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>>> cveOps = getMergedAndDeprecatedCVEOpsMap(processingAccSet);

            Map<Long, List<EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>> temp2OrgAccMergeIntoMap = new HashMap<>();
            // update mergedInto and deprecated
            orgAccMergeIntoMap.forEach((orgAcc, mergeIntoList) -> {
                List<Long> mergeIntoListAcc = mergeIntoList.stream().map(ed -> ed.getMergedInto()).collect(Collectors.toList());
                List<EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>> listOfEventDocs = cveOps.entrySet().stream()
                        .filter(entry -> mergeIntoListAcc.contains(entry.getKey()))
                        .flatMap(entry -> entry.getValue().values().stream().flatMap(Collection::stream))
                        .collect(Collectors.toList());
                List<EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>> newMergedIntoAcc = listOfEventDocs.stream()
                        .filter(eventDoc -> eventDoc.getEventType().equals(EventType.MERGED))
                        .collect(Collectors.toList());
                Set<Long> deprecatedAccessions = listOfEventDocs.stream()
                        .filter(eventDoc -> eventDoc.getEventType().equals(EventType.DEPRECATED))
                        .map(eventDoc -> eventDoc.getAccession())
                        .collect(Collectors.toSet());

                // log and remove already processed accessions from further processing to avoid loops/circular paths
                Set<Long> newMergedIntoAccessions = newMergedIntoAcc.stream().map(ed -> ed.getMergedInto()).collect(Collectors.toSet());
                Set<Long> alreadyProcessedAcc = alreadyProcessedAccessionsMap.get(orgAcc).stream().filter(acc -> newMergedIntoAccessions.contains(acc)).collect(Collectors.toSet());
                if (!alreadyProcessedAcc.isEmpty()) {
                    logger.error("Loop Found in the merge chain for accession {}. Duplicate accessions {}", orgAcc, alreadyProcessedAcc);
                    newMergedIntoAcc = newMergedIntoAcc.stream().filter(ed -> !alreadyProcessedAcc.contains(ed.getMergedInto())).collect(Collectors.toList());
                }

                if (!newMergedIntoAcc.isEmpty()) {
                    temp2OrgAccMergeIntoMap.put(orgAcc, newMergedIntoAcc);
                }
                if (!deprecatedAccessions.isEmpty()) {
                    mergedIntoDeprecatedAccessionsMap.get(orgAcc).addAll(deprecatedAccessions);
                }
            });

            // check and report accessions if an accession is both active and also has merged/deprecated operations
            Set<Long> bothActiveMergedDeprecatedAcc = cveOps.keySet().stream()
                    .filter(acc -> activeAccSet.contains(acc))
                    .collect(Collectors.toSet());
            if (!bothActiveMergedDeprecatedAcc.isEmpty()) {
                logger.warn("The following accessions are both Active and have Merged/Deprecated operations: {}", bothActiveMergedDeprecatedAcc);
            }

            // check and log accessions which are neither active nor has any further merge/deprecated operations
            orgAccMergeIntoMap.forEach((orgAcc, mergeIntoList) -> {
                mergeIntoList.forEach(eventDoc -> {
                    if (!activeAccSet.contains(eventDoc.getMergedInto()) && !cveOps.containsKey(eventDoc.getMergedInto())) {
                        logger.warn("Accession is Neither Active nor has any further Merged/Deprecated Operations. Original Accessions {}, Accession for which nothing found: {}", orgAcc, eventDoc.getMergedInto());
                    }
                });
            });

            // update processing set
            orgAccMergeIntoMap = temp2OrgAccMergeIntoMap;
            processingAccSet = orgAccMergeIntoMap.values().stream()
                    .flatMap(Collection::stream)
                    .map(EventDocument::getMergedInto)
                    .collect(Collectors.toSet());

        } // end of processing loop

    }

    private Map<Long, Map<Boolean, List<EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>>> getMergedAndDeprecatedCVEOpsMap(Set<Long> cveAccList) {
        return getMergedAndDeprecatedCVEOps(cveAccList)
                .stream()
                .collect(Collectors.groupingBy(EventDocument::getAccession,
                        Collectors.partitioningBy(event -> event.getEventType().equals(EventType.MERGED))));

    }

    private List<EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>> getMergedAndDeprecatedCVEOps(Set<Long> cveAccList) {
        Query query = query(where(CVE_OPS_EVENT_TYPE_FIELD).in(EventType.MERGED.toString(), EventType.DEPRECATED.toString())
                .and(CVE_OPS_ACCESSION_FIELD).in(cveAccList).and(CVE_OPS_ASM_FIELD).is(assembly));
        List<ClusteredVariantOperationEntity> evaOpsResults = mongoTemplate.find(query, ClusteredVariantOperationEntity.class);
        List<DbsnpClusteredVariantOperationEntity> dbsnpOpsResults = mongoTemplate.find(query, DbsnpClusteredVariantOperationEntity.class);
        return Stream.concat(evaOpsResults.stream(), dbsnpOpsResults.stream()).collect(Collectors.toList());
    }

    private Map<Long, List<EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>> getSVEOpsMap(Set<Long> cveAccs) {
        return getSVEOps(cveAccs)
                .stream()
                .collect(Collectors.groupingBy(sveOps -> sveOps.getInactiveObjects().iterator().next().getClusteredVariantAccession()));

    }

    private List<EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>> getSVEOps(Set<Long> cveAccs) {
        Query query = query(where(SVE_OPS_EVENT_TYPE_FIELD).is(EventType.UPDATED.toString())
                .and(SVE_OPS_RS_FIELD).in(cveAccs)
                .and(SVE_OPS_EVENT_ASM_FIELD).is(assembly)
                .and(SVE_OPS_EVENT_TAX_FIELD).is(taxonomy));
        List<SubmittedVariantOperationEntity> evaOpsResults = mongoTemplate.find(query, SubmittedVariantOperationEntity.class);
        List<DbsnpSubmittedVariantOperationEntity> dbsnpOpsResults = mongoTemplate.find(query, DbsnpSubmittedVariantOperationEntity.class);
        return Stream.concat(evaOpsResults.stream(), dbsnpOpsResults.stream()).collect(Collectors.toList());
    }

    private List<ClusteredVariantEntity> getClusteredVariantEntities(Set<Long> cveAccs) {
        Query query = query(where(CVE_ACC_FIELD).in(cveAccs).and(CVE_ASM_FIELD).is(assembly));
        List<ClusteredVariantEntity> evaResults = mongoTemplate.find(query, ClusteredVariantEntity.class);
        List<DbsnpClusteredVariantEntity> dbsnpResults = mongoTemplate.find(query, DbsnpClusteredVariantEntity.class);
        return Stream.concat(evaResults.stream(), dbsnpResults.stream()).collect(Collectors.toList());
    }

    protected List<Variant> getVariants(EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity> cveOp,
                                        List<EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>> sveOpsEntityList) {
        List<? extends ClusteredVariantInactiveEntity> inactiveObjects = cveOp.getInactiveObjects();
        if (inactiveObjects.size() > 1) {
            throw new AssertionError("The class '" + this.getClass().getSimpleName()
                    + "' was designed assuming there's only one element in the field " + "'inactiveObjects'. " +
                    "Found " + inactiveObjects.size() + " elements in _id=" + cveOp.getAccession());
        }
        ClusteredVariantInactiveEntity inactiveEntity = inactiveObjects.iterator().next();
        String contig = inactiveEntity.getContig();
        long start = inactiveEntity.getStart();
        long rs = inactiveEntity.getAccession();
        String type = inactiveEntity.getType().toString();
        String sequenceOntology = VariantTypeToSOAccessionMap.getSequenceOntologyAccession(VariantType.valueOf(type));
        boolean validated = inactiveEntity.isValidated() != null ? inactiveEntity.isValidated() : false;

        Map<String, Variant> mergedVariants = new HashMap<>();

        boolean remappedRS = sveOpsEntityList
                .stream()
                .map(e -> e.getInactiveObjects())
                .flatMap(Collection::stream)
                .allMatch(sve -> Objects.nonNull(sve.getRemappedFrom()));

        boolean hasSubmittedVariantsDeclustered = false;
        for (EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity> submittedVariantOperation : sveOpsEntityList) {
            if (submittedVariantOperation.getEventType().equals(EventType.UPDATED)
                    && submittedVariantOperation.getReason().startsWith(MERGE_OPERATION_REASON_FIRST_WORD)) {
                List<? extends SubmittedVariantInactiveEntity> inactiveEntitySubmittedVariant = submittedVariantOperation.getInactiveObjects();
                SubmittedVariantInactiveEntity submittedVariant = inactiveEntitySubmittedVariant.iterator().next();
                long submittedVariantStart = submittedVariant.getStart();
                String submittedVariantContig = submittedVariant.getContig();

                if (!isSameLocation(contig, start, submittedVariantContig, submittedVariantStart, type)) {
                    continue;
                }

                String reference = submittedVariant.getReferenceAllele();
                String alternate = submittedVariant.getAlternateAllele();
                String study = submittedVariant.getProjectAccession();
                boolean submittedVariantValidated = submittedVariant.isValidated();
                boolean allelesMatch = submittedVariant.isAllelesMatch();
                boolean assemblyMatch = submittedVariant.isAssemblyMatch();
                boolean evidence = submittedVariant.isSupportedByEvidence();

                VariantSourceEntry sourceEntry = buildVariantSourceEntry(study, sequenceOntology, validated,
                        submittedVariantValidated, allelesMatch, assemblyMatch, evidence, remappedRS, cveOp.getMergedInto());

                addToVariants(mergedVariants, contig, submittedVariantStart, rs, reference, alternate, sourceEntry);
            } else if (submittedVariantOperation.getEventType().equals(EventType.UPDATED)
                    && submittedVariantOperation.getReason().startsWith(DECLUSTER_OPERATION_REASON_FIRST_WORD)) {
                hasSubmittedVariantsDeclustered = true;
            }
        }

        if (!hasSubmittedVariantsDeclustered && mergedVariants.isEmpty()) {
            logger.warn("Found merge operation for rs" + rs + " but no SS IDs updates "
                    + "(merge/update) in the collection containing operations. "
                    + "This could have possibly happened on a remapped variant "
                    + "because there was a subsequent split issued for this RS due to loci disagreement "
                    + "between the RS and the SS.");
            return Collections.emptyList();
        }

        return new ArrayList<>(mergedVariants.values());
    }

    protected VariantSourceEntry buildVariantSourceEntry(String study, String sequenceOntology, boolean validated,
                                                         boolean submittedVariantValidated, boolean allelesMatch,
                                                         boolean assemblyMatch, boolean evidence, boolean remappedRS,
                                                         Long mergedInto) {
        VariantSourceEntry variantSourceEntry = buildVariantSourceEntry(study, sequenceOntology, validated,
                submittedVariantValidated, allelesMatch,
                assemblyMatch, evidence, remappedRS);
        if (Objects.nonNull(mergedInto)) {
            variantSourceEntry.addAttribute(MERGED_INTO_KEY, buildId(mergedInto));
        }
        return variantSourceEntry;
    }


    protected VariantSourceEntry buildVariantSourceEntry(String study, String sequenceOntology, boolean validated,
                                                         boolean submittedVariantValidated, boolean allelesMatch,
                                                         boolean assemblyMatch, boolean evidence, boolean remappedRS) {
        VariantSourceEntry sourceEntry = new VariantSourceEntry(study, study);
        sourceEntry.addAttribute(VARIANT_CLASS_KEY, sequenceOntology);
        sourceEntry.addAttribute(STUDY_ID_KEY, study);
        sourceEntry.addAttribute(CLUSTERED_VARIANT_VALIDATED_KEY, Boolean.toString(validated));
        sourceEntry.addAttribute(SUBMITTED_VARIANT_VALIDATED_KEY, Boolean.toString(submittedVariantValidated));
        sourceEntry.addAttribute(ALLELES_MATCH_KEY, Boolean.toString(allelesMatch));
        sourceEntry.addAttribute(ASSEMBLY_MATCH_KEY, Boolean.toString(assemblyMatch));
        sourceEntry.addAttribute(SUPPORTED_BY_EVIDENCE_KEY, Boolean.toString(evidence));
        sourceEntry.addAttribute(REMAPPED_KEY, Boolean.toString(remappedRS));
        return sourceEntry;
    }

    private void addToVariants(Map<String, Variant> variants, String contig, long start, long rs, String reference,
                               String alternate, VariantSourceEntry sourceEntry) {
        String variantId = (contig + "_" + start + "_" + reference + "_" + alternate).toUpperCase();
        if (variants.containsKey(variantId)) {
            variants.get(variantId).addSourceEntry(sourceEntry);
        } else {
            long end = calculateEnd(reference, alternate, start);
            Variant variant = new Variant(contig, start, end, reference, alternate);
            variant.setMainId(buildId(rs));
            variant.setIds(Collections.singleton(buildId(rs)));
            variant.addSourceEntry(sourceEntry);
            variants.put(variantId, variant);
        }
    }

    private long calculateEnd(String reference, String alternate, long start) {
        long length = Math.max(reference.length(), alternate.length());
        return start + length - 1;
    }


    private String buildId(long rs) {
        return RS_PREFIX + rs;
    }

    protected boolean isSameLocation(String contig, long start, String submittedVariantContig, long submittedVariantStart, String type) {
        return contig.equals(submittedVariantContig) && isSameStart(start, submittedVariantStart, type);
    }

    private boolean isSameStart(long clusteredVariantStart, long submittedVariantStart, String type) {
        return clusteredVariantStart == submittedVariantStart || (isIndel(type) && Math.abs(clusteredVariantStart - submittedVariantStart) == 1L);
    }

    private boolean isIndel(String type) {
        return type.equals(VariantType.INS.toString()) || type.equals(VariantType.DEL.toString());
    }


    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    private void writeMergeDeprecatedAccessionsToFile(Set<Long> mergedDeprecatedAccessions) {
        for (Long acc : mergedDeprecatedAccessions) {
            try {
                writer.write("rs" + acc + "\n");
            } catch (IOException e) {
                throw new RuntimeException("Error writing Merged Deprecated Accessions to File");
            }
        }
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            if (reader != null) {
                reader.close();
            }
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            throw new ItemStreamException("Failed to close file: " + rsAccFile, e);
        }
    }
}

