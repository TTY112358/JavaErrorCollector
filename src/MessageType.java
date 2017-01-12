import java.io.PrintStream;

/**
 * Created by TTY on 2016/8/2.
 */
public class MessageType {
    int category;
    int type;
    int subtype;
    String description;
    String regex;
    PrintStream detailMessageWriter;

    public MessageType(int category, int type, int subtype, String description, String regex) {
        this.category = category;
        this.type = type;
        this.subtype = subtype;
        this.description = description;
        this.regex = regex;
    }

    public void printInfo() {
        System.out.println("category: " + category);
        System.out.println("type: " + type);
        System.out.println("subtype: " + subtype);
        System.out.println("description: " + description);
        System.out.println("regex: " + regex);
    }
}
