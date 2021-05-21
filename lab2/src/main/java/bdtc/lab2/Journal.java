package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Journal {
    private int year;
    private int univId;
    private int userId;
    private long checktime;
}
