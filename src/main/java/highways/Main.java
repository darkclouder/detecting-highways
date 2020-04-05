package highways;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        if (args.length == 0) {
            printHelp();
            return;
        }

        String[] subArgs = Arrays.copyOfRange(args, 1, args.length);

        switch (args[0]) {
            case "file":
                FileTasks.main(subArgs);
                break;
            case "postgres":
                PostgresTasks.main(subArgs);
                break;
            default:
                printHelp();
        }
    }

    private static void printHelp() {
        System.out.println("Usage:");
        FileTasks.printCommandHelp();
        PostgresTasks.printCommandHelp();
    }
}
