import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.time.StopWatch;

public class Sandbox {

    public static void main(String[] args) {

        @RequiredArgsConstructor
        class Dog{
            @NonNull String name;

            public String toString(){
                return getClass().getSimpleName();
            }
        }

        Dog dog = new Dog("Fido");
        System.out.println(dog.toString());
    }
}
