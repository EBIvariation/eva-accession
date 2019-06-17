/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.dbsnp.io;

import org.springframework.jdbc.core.RowMapper;

import uk.ac.ebi.eva.accession.dbsnp.model.CoordinatesPresence;

import java.sql.ResultSet;
import java.sql.SQLException;

import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsContigAndChromosomeReader.CHROMOSOME_NAME_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsContigAndChromosomeReader.CHROMOSOME_START_PRESENT_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsContigAndChromosomeReader.CONTIG_NAME_COLUMN;

public class SubSnpNoHgvsContigAndChromosomeRowMapper implements RowMapper<CoordinatesPresence> {

    @Override
    public CoordinatesPresence mapRow(ResultSet resultSet, int i) throws SQLException {
        return new CoordinatesPresence(resultSet.getString(CHROMOSOME_NAME_COLUMN),
                                       resultSet.getBoolean(CHROMOSOME_START_PRESENT_COLUMN),
                                       resultSet.getString(CONTIG_NAME_COLUMN));
    }
}
