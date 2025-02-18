package uk.ac.ebi.eva.accession.release.batch.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.models.VariantTypeToSOAccessionMap;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
public class ActiveAccessionsVariantReader implements ItemStreamReader<List<Variant>> {
    private static final Logger logger = LoggerFactory.getLogger(ActiveAccessionsVariantReader.class);

    private static final String CVE_ACC_FIELD = "accession";
    private static final String CVE_ASSEMBLY_FIELD = "asm";
    private static final String SVE_RS_FIELD = "rs";
    private static final String SVE_ASSEMBLY_FIELD = "seq";
    private static final String SVE_TAX_FIELD = "tax";

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

    private BufferedReader reader;

    public ActiveAccessionsVariantReader(MongoTemplate mongoTemplate, String rsAccFile, String assembly, int taxonomy,
                                         int chunkSize) {
        this.mongoTemplate = mongoTemplate;
        this.assembly = assembly;
        this.taxonomy = taxonomy;
        this.rsAccFile = rsAccFile;
        this.chunkSize = chunkSize;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            reader = new BufferedReader(new FileReader(rsAccFile));
        } catch (IOException e) {
            throw new ItemStreamException("Failed to open the file (" + rsAccFile + ") with clustered variant accessions", e);
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
                return processCveAccession(cveAccList);
            }
        } catch (IOException e) {
            throw new ItemStreamException("Error reading variant Accessions from file", e);
        }
    }


    public List<Variant> processCveAccession(List<Long> cveAccList) {
        Set<Long> cveAccSet = new HashSet<>(cveAccList);

        List<ClusteredVariantEntity> clusteredVariantEntityList = getClusteredVariantEntities(cveAccSet);
        List<SubmittedVariantEntity> submittedVariantEntityList = getSubmittedVariantEntities(cveAccSet);

        Map<Long, List<SubmittedVariantEntity>> submittedVariantEntityMap = submittedVariantEntityList.stream()
                .collect(Collectors.groupingBy(SubmittedVariantEntity::getClusteredVariantAccession));

        List<Variant> variantList = new ArrayList<>();

        for (ClusteredVariantEntity clusteredVariant : clusteredVariantEntityList) {
            List<SubmittedVariantEntity> submittedVariantEntities = submittedVariantEntityMap.get(clusteredVariant.getAccession());
            if (submittedVariantEntities != null) {
                variantList.addAll(getVariants(clusteredVariant, submittedVariantEntities));
            }
        }

        return variantList;
    }

    private List<ClusteredVariantEntity> getClusteredVariantEntities(Set<Long> cveAccs) {
        Query query = query(where(CVE_ACC_FIELD).in(cveAccs).and(CVE_ASSEMBLY_FIELD).is(assembly));
        List<ClusteredVariantEntity> evaResults = mongoTemplate.find(query, ClusteredVariantEntity.class);
        List<DbsnpClusteredVariantEntity> dbsnpResults = mongoTemplate.find(query, DbsnpClusteredVariantEntity.class);
        return Stream.concat(evaResults.stream(), dbsnpResults.stream()).collect(Collectors.toList());
    }

    private List<SubmittedVariantEntity> getSubmittedVariantEntities(Set<Long> cveAccs) {
        Query query = query(where(SVE_RS_FIELD).in(cveAccs).and(SVE_ASSEMBLY_FIELD).is(assembly).and(SVE_TAX_FIELD).is(taxonomy));
        List<SubmittedVariantEntity> evaResults = mongoTemplate.find(query, SubmittedVariantEntity.class);
        List<DbsnpSubmittedVariantEntity> dbsnpResults = mongoTemplate.find(query, DbsnpSubmittedVariantEntity.class);
        return Stream.concat(evaResults.stream(), dbsnpResults.stream()).collect(Collectors.toList());
    }

    private List<Variant> getVariants(ClusteredVariantEntity clusteredVariant, List<SubmittedVariantEntity> submittedVariants) {
        String contig = clusteredVariant.getContig();
        long start = clusteredVariant.getStart();
        long rs = clusteredVariant.getAccession();
        String type = clusteredVariant.getType().toString();
        String sequenceOntology = VariantTypeToSOAccessionMap.getSequenceOntologyAccession(VariantType.valueOf(type));
        boolean validated = clusteredVariant.isValidated();

        Map<String, Variant> variants = new HashMap<>();

        boolean remappedRS = submittedVariants.stream()
                .allMatch(sve -> Objects.nonNull(sve.getRemappedFrom()));

        for (SubmittedVariantEntity submittedVariant : submittedVariants) {
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
                    submittedVariantValidated, allelesMatch,
                    assemblyMatch, evidence, remappedRS);

            addToVariants(variants, contig, submittedVariantStart, rs, reference, alternate, sourceEntry);
        }
        return new ArrayList<>(variants.values());
    }

    protected VariantSourceEntry buildVariantSourceEntry(String study, String sequenceOntology, boolean validated,
                                                         boolean submittedVariantValidated, boolean allelesMatch,
                                                         boolean assemblyMatch, boolean evidence,
                                                         boolean remappedRS) {
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

    /**
     * The query performed in mongo can retrieve more variants than the actual ones because in some cases the same
     * clustered variant is mapped against multiple locations. So we need to check that that clustered variant we are
     * processing only appears in the VCF release file with the alleles from submitted variants matching the location.
     */
    protected boolean isSameLocation(String contig, long start, String submittedVariantContig,
                                     long submittedVariantStart,
                                     String type) {
        return contig.equals(submittedVariantContig) && isSameStart(start, submittedVariantStart, type);
    }

    /**
     * The start is considered to be the same when:
     * - start in clustered and submitted variant match
     * - start in clustered and submitted variant have a difference of 1
     * <p>
     * The start position can be different in ambiguous INDELS because the renormalization is only applied to
     * submitted variants. In those cases the start in the clustered and submitted variants will not exactly match but
     * the difference should be 1
     * <p>
     * Example:
     * RS (assembly: GCA_000309985.1, accession: 268233057, chromosome: CM001642.1, start: 7356605, type: INS)
     * SS (assembly: GCA_000309985.1, accession: 490570267, chromosome: CM001642.1, start: 7356604, reference: ,
     * alternate: AGAGCTATGATCTTCGGAAGGAGAAGGAGAAGGAAAAGATTCATGACGTCCAC)
     */
    private boolean isSameStart(long clusteredVariantStart, long submittedVariantStart, String type) {
        return clusteredVariantStart == submittedVariantStart
                || (isIndel(type) && Math.abs(clusteredVariantStart - submittedVariantStart) == 1L);
    }

    private boolean isIndel(String type) {
        return type.equals(VariantType.INS.toString()) || type.equals(VariantType.DEL.toString());
    }


    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            throw new ItemStreamException("Failed to close file: " + rsAccFile, e);
        }
    }
}

