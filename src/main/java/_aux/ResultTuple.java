package _aux;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class ResultTuple {
    @NonNull @Getter public List<Integer> LHS;
    @NonNull @Getter public List<Integer> RHS;
    @NonNull @Getter public List<String> lHeaders;
    @NonNull @Getter public List<String> rHeaders;
    @NonNull @Getter public double similarity;

    public String toString() {
        return String.format("%s | %s -> %.3f",
                LHS.stream().map(Object::toString).collect(Collectors.joining("-")),
                RHS.stream().map(Object::toString).collect(Collectors.joining("-")),
                similarity
        );
    }

    @Override
    public boolean equals(Object other){
        if (other == null) return false;
        ResultTuple otherTuple = (ResultTuple) other;
        if (LHS.size() != otherTuple.LHS.size() && RHS.size() != otherTuple.RHS.size()){
            return false;
        }

        boolean equalSides = LHS.size() == RHS.size();

        // check LHS
        for (int i = 0; i < LHS.size(); i++){
            Integer lEntry = LHS.get(i);
            if (!lEntry.equals(otherTuple.LHS.get(i))){
                if (equalSides && !lEntry.equals(otherTuple.RHS.get(i))) { // check for symmetry
                    return false;
                }
            }
        }

        // check RHS
        for (int i = 0; i < RHS.size(); i++){
            Integer rEntry = RHS.get(i);
            if (!rEntry.equals(otherTuple.RHS.get(i))){
                if (equalSides && !rEntry.equals(otherTuple.LHS.get(i))) { // check for symmetry
                    return false;
                }
            }
        }
        return true;
    }
}
