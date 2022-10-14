package _aux;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
public class ResultTuple {
    @NonNull @Getter public List<Integer> LHS;
    @NonNull @Getter public List<Integer> RHS;
    @NonNull @Getter public List<String> lHeaders;
    @NonNull @Getter public List<String> rHeaders;
    @NonNull @Getter public double similarity;

}
