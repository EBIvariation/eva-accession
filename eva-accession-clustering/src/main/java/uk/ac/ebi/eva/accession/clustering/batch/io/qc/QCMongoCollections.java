package uk.ac.ebi.eva.accession.clustering.batch.io.qc;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

public class QCMongoCollections {

    // remove dots and underscores in the assembly accession because extractUniqueHashesForDuplicateKeyError
    // cannot detect anything other than alphanumeric characters
    public final static String getAssemblyAccessionPrefix(String assemblyAccession) {
      return assemblyAccession.replace(".", "").replace("_", "");
    }

    // QC Collection that collects all the RS hashes in the submitted variants for an assembly
    @Document
    public static class qcRSHashInSS {
        // In the format <assembly>#<rs hash>#<rs ID>, optimizes lookup by hash
        // GCA_000181335.4#FAB042ED1B2C6EC7DD2115EFEFD71EE8026CBB13
        @Id
        private final String id;

        private final Long rsID;

        public qcRSHashInSS(String id, Long rsID) {
            this.id = id;
            this.rsID = rsID;
        }

        public String getId() {
            return id;
        }

        public Long getRsID() {
            return rsID;
        }
    }

    // QC Collection that collects all the RS IDs in the submitted variants for an assembly
    @Document
    public static class qcRSIdInSS {
        // In the format <assembly>#<rs ID>#<rs hash>, optimizes lookup by ID
        // GCA_000181335.4#785193025
        @Id
        private final String id;

        private final String hash;

        public qcRSIdInSS(String id, String hash) {
            this.id = id;
            this.hash = hash;
        }

        public String getId() {
            return id;
        }

        public String getHash() {
            return hash;
        }
    }
}
