/*
 * Copyright 2014-2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.accession.dbsnp.io;

import org.springframework.jdbc.core.RowMapper;

import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantType;
import uk.ac.ebi.eva.accession.dbsnp.model.Orientation;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SubSnpNoHgvsRowMapper implements RowMapper<SubSnpNoHgvs> {

    public static final String SS_ID_COLUMN = "ss_id";

    public static final String RS_ID_COLUMN = "rs_id";

    public static final String ALLELES_COLUMN = "alleles";

    public static final String BATCH_HANDLE_COLUMN = "batch_handle";

    public static final String BATCH_NAME_COLUMN = "batch_name";

    public static final String CHROMOSOME_COLUMN = "chromosome";

    public static final String CHROMOSOME_START_COLUMN = "chromosome_start";

    public static final String CONTIG_NAME_COLUMN = "contig_name";

    public static final String SNP_CLASS_COLUMN = "snp_class";

    public static final String SUBSNP_ORIENTATION_COLUMN = "subsnp_orientation";

    public static final String SNP_ORIENTATION_COLUMN = "snp_orientation";

    public static final String CONTIG_ORIENTATION_COLUMN = "contig_orientation";

    public static final String CONTIG_START_COLUMN = "contig_start";

    public static final String FREQUENCY_EXISTS_COLUMN = "freq_exists";

    public static final String GENOTYPE_EXISTS_COLUMN = "genotype_exists";

    public static final String REFERENCE_COLUMN = "reference";

    public static final String SS_CREATE_TIME_COLUMN = "ss_create_time";

    public static final String TAXONOMY_ID_COLUMN = "tax_id";

    private final String assembly;

    public SubSnpNoHgvsRowMapper(String assembly) {
        this.assembly = assembly;
    }

    @Override
    public SubSnpNoHgvs mapRow(ResultSet resultSet, int i) throws SQLException {
        return new SubSnpNoHgvs(resultSet.getLong(SS_ID_COLUMN),
                                resultSet.getLong(RS_ID_COLUMN),
                                resultSet.getString(ALLELES_COLUMN),
                                assembly,
                                resultSet.getString(BATCH_HANDLE_COLUMN),
                                resultSet.getString(BATCH_NAME_COLUMN),
                                resultSet.getString(CHROMOSOME_COLUMN),
                                resultSet.getLong(CHROMOSOME_START_COLUMN),
                                resultSet.getString(CONTIG_NAME_COLUMN),
                                DbsnpVariantType.getVariantClass(resultSet.getInt(SNP_CLASS_COLUMN)),
                                Orientation.getOrientation(resultSet.getObject(SUBSNP_ORIENTATION_COLUMN, Integer.class)),
                                Orientation.getOrientation(resultSet.getObject(SNP_ORIENTATION_COLUMN, Integer.class)),
                                Orientation.getOrientation(resultSet.getObject(CONTIG_ORIENTATION_COLUMN, Integer.class)),
                                resultSet.getLong(CONTIG_START_COLUMN),
                                resultSet.getBoolean(FREQUENCY_EXISTS_COLUMN),
                                resultSet.getBoolean(GENOTYPE_EXISTS_COLUMN),
                                resultSet.getString(REFERENCE_COLUMN),
                                resultSet.getTimestamp(SS_CREATE_TIME_COLUMN),
                                resultSet.getInt(TAXONOMY_ID_COLUMN));
    }
}
