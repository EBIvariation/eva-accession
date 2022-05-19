package uk.ac.ebi.eva.accession.ws.test;

import org.mockito.ArgumentMatcher;

import uk.ac.ebi.eva.commons.core.models.contigalias.ContigNamingConvention;

public class NoContigTranslationArgumentMatcher implements ArgumentMatcher<ContigNamingConvention> {
    public boolean matches(ContigNamingConvention contigNamingConvention)  {
        return (contigNamingConvention == null
                || contigNamingConvention.equals(ContigNamingConvention.INSDC)
                || contigNamingConvention.equals(ContigNamingConvention.NO_REPLACEMENT));
    }

}
