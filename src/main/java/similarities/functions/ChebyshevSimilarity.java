package similarities.functions;

import _aux.lib;

public class ChebyshevSimilarity extends MinkowskiSimilarity{
    public ChebyshevSimilarity() {
        super(0);
        this.distFunc = lib::chebyshev;
    }
}
