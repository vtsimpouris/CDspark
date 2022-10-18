package similarities.functions;

import _aux.lib;

public class SpearmanCorrelation extends PearsonCorrelation{
    @Override public double[][] preprocess(double[][] data) {
        double[][] ranks = new double[data.length][data[0].length];
        for (int i = 0; i < data.length; i++) {
            ranks[i] = lib.rank(data[i]);
        }
        lib.znorm(ranks);
        return ranks;
    }
}
