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
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * Read all ClusteredVariant Accessions from file in batches
 */
public class DeprecatedAccessionsVariantReader implements ItemStreamReader<List<Variant>> {
    private static final Logger logger = LoggerFactory.getLogger(DeprecatedAccessionsVariantReader.class);


    public static final String CVE_OPS_ACCESSION_FIELD = "accession";
    public static final String CVE_OPS_EVENT_TYPE_FIELD = "eventType";
    public static final String CVE_OPS_ASM_FIELD = "inactiveObjects.asm";

    public static final String SVE_OPS_RS_FIELD = "inactiveObjects.rs";
    public static final String SVE_OPS_EVENT_ASM_FIELD = "inactiveObjects.seq";
    public static final String SVE_OPS_EVENT_TAX_FIELD = "inactiveObjects.tax";
    public static final String SVE_OPS_EVENT_TYPE_FIELD = "eventType";

    private MongoTemplate mongoTemplate;
    private String rsAccFile;
    private String assembly;
    private int taxonomy;
    private int chunkSize;

    private BufferedReader reader;

    public DeprecatedAccessionsVariantReader(MongoTemplate mongoTemplate, String rsAccFile, String assembly, int taxonomy,
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

        List<EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>> cveOpsEntities = getClusteredVariantOperationEntities(cveAccSet);
        List<EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>> sveOpsEntities = getSubmittedVariantOperationEntities(cveAccSet);
        Map<Long, List<EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>> sveOpsEntitiesMap = sveOpsEntities.stream()
                .collect(Collectors.groupingBy(sve -> sve.getInactiveObjects().iterator().next().getClusteredVariantAccession()));

        List<Variant> variantList = new ArrayList<>();
        for (EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity> cveOps : cveOpsEntities) {
            List<EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>> sveOpsEntityList = sveOpsEntitiesMap.get(cveOps.getAccession());
            if (sveOpsEntityList != null) {
                variantList.add(getVariants(cveOps, sveOpsEntityList));
            }
        }

        return variantList;
    }

    private List<EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>> getClusteredVariantOperationEntities(Set<Long> cveAccList) {
        Query query = query(where(CVE_OPS_EVENT_TYPE_FIELD).is(EventType.DEPRECATED.toString())
                .and(CVE_OPS_ASM_FIELD).is(assembly)
                .and(CVE_OPS_ACCESSION_FIELD).in(cveAccList));
        List<ClusteredVariantOperationEntity> evaOpsResults = mongoTemplate.find(query, ClusteredVariantOperationEntity.class);
        List<DbsnpClusteredVariantOperationEntity> dbsnpOpsResults = mongoTemplate.find(query, DbsnpClusteredVariantOperationEntity.class);
        return Stream.concat(evaOpsResults.stream(), dbsnpOpsResults.stream()).collect(Collectors.toList());
    }

    private List<EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>> getSubmittedVariantOperationEntities(Set<Long> cveAccs) {
        Query query = query(where(SVE_OPS_EVENT_TYPE_FIELD).in(EventType.UPDATED.toString(), EventType.DEPRECATED.toString())
                .and(SVE_OPS_RS_FIELD).in(cveAccs)
                .and(SVE_OPS_EVENT_ASM_FIELD).is(assembly)
                .and(SVE_OPS_EVENT_TAX_FIELD).is(taxonomy));
        List<SubmittedVariantOperationEntity> evaOpsResults = mongoTemplate.find(query, SubmittedVariantOperationEntity.class);
        List<DbsnpSubmittedVariantOperationEntity> dbsnpOpsResults = mongoTemplate.find(query, DbsnpSubmittedVariantOperationEntity.class);
        return Stream.concat(evaOpsResults.stream(), dbsnpOpsResults.stream()).collect(Collectors.toList());
    }

    private Variant getVariants(EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity> cveOps,
                                List<EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>> sveOpsEntityList) {
        ClusteredVariantInactiveEntity cveOpsInactiveEntity = cveOps.getInactiveObjects().iterator().next();
        String contig = cveOpsInactiveEntity.getContig();
        long start = cveOpsInactiveEntity.getStart();
        // Since we only need evidence that at least one submitted variant agrees with the deprecated RS,
        // we just return one variant record per RS
        List<? extends SubmittedVariantInactiveEntity> sveOpsInactiveEntityList = sveOpsEntityList.iterator().next().getInactiveObjects();
        SubmittedVariantInactiveEntity sveOpsInactiveEntity = sveOpsInactiveEntityList.iterator().next();
        String reference = sveOpsInactiveEntity.getReferenceAllele();
        String alternate = sveOpsInactiveEntity.getAlternateAllele();

        Variant variantToReturn = new Variant(contig, start,
                start + Math.max(reference.length(), alternate.length()) - 1,
                reference, alternate);
        variantToReturn.setMainId("rs" + cveOps.getAccession());

        return variantToReturn;
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

