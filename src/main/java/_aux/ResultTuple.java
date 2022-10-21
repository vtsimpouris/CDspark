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

    @Override
    public boolean equals(Object other){
        if (other == null) return false;
        ResultTuple otherTuple = (ResultTuple) other;
        if (LHS.size() != otherTuple.LHS.size() && RHS.size() != otherTuple.RHS.size()){
            return false;
        }
        // check LHS
        for (int i = 0; i < LHS.size(); i++){
            if (!LHS.get(i).equals(otherTuple.LHS.get(i))){
                return false;
            }
        }

        // check RHS
        for (int i = 0; i < RHS.size(); i++){
            if (!RHS.get(i).equals(otherTuple.RHS.get(i))){
                return false;
            }
        }
        return true;
    }
}
