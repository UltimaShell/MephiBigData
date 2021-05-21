package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Publications {
    private int year;
    private int userId;
    private int univId;
}
