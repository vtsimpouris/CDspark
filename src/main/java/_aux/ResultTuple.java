package _aux;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ResultTuple {
    @NonNull @Getter public int[] LHS;
    @NonNull @Getter public int[] RHS;
    @NonNull @Getter public int[] lHeaders;
    @NonNull @Getter public int[] rHeaders;
    @NonNull @Getter public double similarity;

}
