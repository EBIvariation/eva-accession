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
package uk.ac.ebi.eva.accession.dbsnp.model;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class DbsnpVariantAllelesTest {

    @Test
    public void forwardAllelesAndReference() {
        DbsnpVariantAlleles forwardAllelesMNV = new DbsnpVariantAlleles("TA", "TG/TA/GG", Orientation.FORWARD,
                                                                        Orientation.FORWARD, DbsnpVariantType.MNV);

        assertEquals("TA", forwardAllelesMNV.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("TG", "TA", "GG"), forwardAllelesMNV.getAllelesInForwardStrand());

        DbsnpVariantAlleles forwardAllelesSNV = new DbsnpVariantAlleles("T", "T/G", Orientation.FORWARD,
                                                                        Orientation.FORWARD, DbsnpVariantType.SNV);

        assertEquals("T", forwardAllelesSNV.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("T", "G"), forwardAllelesSNV.getAllelesInForwardStrand());

        DbsnpVariantAlleles forwardAllelesInsertion = new DbsnpVariantAlleles("-", "-/T", Orientation.FORWARD,
                                                                        Orientation.FORWARD, DbsnpVariantType.DIV);

        assertEquals("", forwardAllelesInsertion.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("", "T"), forwardAllelesInsertion.getAllelesInForwardStrand());

        DbsnpVariantAlleles forwardAllelesDeletion = new DbsnpVariantAlleles("TC", "-/TC", Orientation.FORWARD,
                                                                        Orientation.FORWARD, DbsnpVariantType.DIV);

        assertEquals("TC", forwardAllelesDeletion.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("", "TC"), forwardAllelesDeletion.getAllelesInForwardStrand());

        DbsnpVariantAlleles notEmptyReferenceInsertion = new DbsnpVariantAlleles("T", "T/TC", Orientation.FORWARD,
                                                                        Orientation.FORWARD, DbsnpVariantType.DIV);

        assertEquals("T", notEmptyReferenceInsertion.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("T", "TC"), notEmptyReferenceInsertion.getAllelesInForwardStrand());

        DbsnpVariantAlleles notEmptyReferenceDeletion = new DbsnpVariantAlleles("TGA", "T/TGA", Orientation.FORWARD,
                                                                        Orientation.FORWARD, DbsnpVariantType.DIV);

        assertEquals("TGA", notEmptyReferenceDeletion.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("T", "TGA"), notEmptyReferenceDeletion.getAllelesInForwardStrand());
    }

    @Test
    public void forwardAllelesReverseReference() {
        DbsnpVariantAlleles forwardAllelesMNV = new DbsnpVariantAlleles("TC", "TG/TC/GG", Orientation.REVERSE,
                                                                        Orientation.FORWARD, DbsnpVariantType.MNV);

        assertEquals("GA", forwardAllelesMNV.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("TG", "TC", "GG"), forwardAllelesMNV.getAllelesInForwardStrand());

        DbsnpVariantAlleles forwardAllelesSNV = new DbsnpVariantAlleles("T", "T/G", Orientation.REVERSE,
                                                                        Orientation.FORWARD, DbsnpVariantType.SNV);

        assertEquals("A", forwardAllelesSNV.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("T", "G"), forwardAllelesSNV.getAllelesInForwardStrand());

        DbsnpVariantAlleles forwardAllelesInsertion = new DbsnpVariantAlleles("-", "-/TC", Orientation.REVERSE,
                                                                              Orientation.FORWARD, DbsnpVariantType.DIV);

        assertEquals("", forwardAllelesInsertion.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("", "TC"), forwardAllelesInsertion.getAllelesInForwardStrand());

        DbsnpVariantAlleles forwardAllelesDeletion = new DbsnpVariantAlleles("AG", "-/CT", Orientation.REVERSE,
                                                                             Orientation.FORWARD, DbsnpVariantType.DIV);

        assertEquals("CT", forwardAllelesDeletion.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("", "CT"), forwardAllelesDeletion.getAllelesInForwardStrand());
    }

    @Test
    public void reverseAllelesForwardReference() {
        DbsnpVariantAlleles forwardAllelesMNV = new DbsnpVariantAlleles("TC", "TG/TC/GG", Orientation.FORWARD,
                                                                        Orientation.REVERSE, DbsnpVariantType.MNV);

        assertEquals("TC", forwardAllelesMNV.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("CA", "GA", "CC"), forwardAllelesMNV.getAllelesInForwardStrand());

        DbsnpVariantAlleles forwardAllelesSNV = new DbsnpVariantAlleles("T", "T/G", Orientation.FORWARD,
                                                                        Orientation.REVERSE, DbsnpVariantType.SNV);

        assertEquals("T", forwardAllelesSNV.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("A", "C"), forwardAllelesSNV.getAllelesInForwardStrand());

        DbsnpVariantAlleles forwardAllelesInsertion = new DbsnpVariantAlleles("-", "-/TC", Orientation.FORWARD,
                                                                              Orientation.REVERSE,
                                                                              DbsnpVariantType.DIV);

        assertEquals("", forwardAllelesInsertion.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("", "GA"), forwardAllelesInsertion.getAllelesInForwardStrand());

        DbsnpVariantAlleles forwardAllelesDeletion = new DbsnpVariantAlleles("AG", "-/CT", Orientation.FORWARD,
                                                                             Orientation.REVERSE, DbsnpVariantType.DIV);

        assertEquals("AG", forwardAllelesDeletion.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("", "AG"), forwardAllelesDeletion.getAllelesInForwardStrand());
    }

    @Test
    public void reverseAllelesAndReference() {
        DbsnpVariantAlleles forwardAllelesMNV = new DbsnpVariantAlleles("TC", "TG/TC/GG", Orientation.REVERSE,
                                                                        Orientation.REVERSE, DbsnpVariantType.MNV);

        assertEquals("GA", forwardAllelesMNV.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("CA", "GA", "CC"), forwardAllelesMNV.getAllelesInForwardStrand());

        DbsnpVariantAlleles forwardAllelesSNV = new DbsnpVariantAlleles("T", "T/G", Orientation.REVERSE,
                                                                        Orientation.REVERSE, DbsnpVariantType.SNV);

        assertEquals("A", forwardAllelesSNV.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("A", "C"), forwardAllelesSNV.getAllelesInForwardStrand());

        DbsnpVariantAlleles forwardAllelesInsertion = new DbsnpVariantAlleles("-", "-/TC", Orientation.REVERSE,
                                                                              Orientation.REVERSE,
                                                                              DbsnpVariantType.DIV);

        assertEquals("", forwardAllelesInsertion.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("", "GA"), forwardAllelesInsertion.getAllelesInForwardStrand());

        DbsnpVariantAlleles forwardAllelesDeletion = new DbsnpVariantAlleles("AG", "-/AG", Orientation.REVERSE,
                                                                             Orientation.REVERSE, DbsnpVariantType.DIV);

        assertEquals("CT", forwardAllelesDeletion.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("", "CT"), forwardAllelesDeletion.getAllelesInForwardStrand());
    }

    @Test
    public void forwardSTRAlleles() {
        DbsnpVariantAlleles forwardAllelesMNV = new DbsnpVariantAlleles("T", "(T)4/5/7", Orientation.FORWARD,
                                                                        Orientation.FORWARD,
                                                                        DbsnpVariantType.MICROSATELLITE);

        assertEquals("T", forwardAllelesMNV.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("(T)4", "(T)5", "(T)7"), forwardAllelesMNV.getAllelesInForwardStrand());
    }

    @Test
    public void reverseSTRAlleles() {
        DbsnpVariantAlleles forwardAllelesMNV = new DbsnpVariantAlleles("AT", "(A)2(TC)8/(TA)3", Orientation.FORWARD,
                                                                        Orientation.REVERSE,
                                                                        DbsnpVariantType.MICROSATELLITE);

        assertEquals("AT", forwardAllelesMNV.getReferenceInForwardStrand());
        assertEquals(Arrays.asList("(GA)8(T)2", "(TA)3"), forwardAllelesMNV.getAllelesInForwardStrand());
    }
}