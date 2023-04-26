import java.sql.SQLException;
import java.util.Scanner;

public class Driver {
    public static void main(String[] args) throws SQLException {
        try {
            Scanner scanner = new Scanner(System.in);
            String URL = args[0];
            String username = args[1];
            String password = args[2];
            System.out.println("Welcome to the university database. Queries available: ");
            System.out.println("1. Search students by name.");
            System.out.println("2. Search students by year.");
            System.out.println("3. Search for students with a GPA >= threshold.");
            System.out.println("4. Search for students with a GPA <= threshold");
            System.out.println("5. Get department statistics.");
            System.out.println("6. Get class statistics.");
            System.out.println("7. Execute an arbitrary SQL query.");
            System.out.println("8. Exit the application.");
            boolean query = true;
            while(query){
                System.out.println("Which query would you like to run? (1-8)");
                int run = scanner.nextInt();
                switch(run){
                    case 1:
                        System.out.println("Please enter the name.");
                        String name = scanner.next();
                        Queries.searchStudentsByName(name, URL, username, password);
                        break;
                    case 2:
                        System.out.println("Please enter the year.");
                        String year = scanner.next();
                        Queries.searchStudentsByYear(year, URL, username, password);
                        break;
                    case 3:
                        System.out.println("Please enter the threshold.");
                        double aboveGPA = scanner.nextDouble();
                        Queries.searchStudentsAboveGPA(aboveGPA, URL, username, password);
                        break;
                    case 4:
                        System.out.println("Please enter the threshold.");
                        double belowGPA = scanner.nextDouble();
                        Queries.searchStudentsBelowGPA(belowGPA, URL, username, password);
                        break;
                    case 5:
                        System.out.println("Please enter the department.");
                        String dept = scanner.next();
                        Queries.searchAverageDepartmentGPA(dept, URL, username, password);
                        break;
                    case 6:
                        System.out.println("Please enter the class name.");
                        String className = scanner.next();
                        Queries.searchClass(className, URL, username, password);
                        break;
                    case 7:
                        System.out.println("Please enter the query.");
                        String sql = scanner.next();
                        sql += scanner.nextLine();
                        Queries.executeQuery(sql, URL, username, password);
                        break;
                    case 8:
                        System.out.println("Goodbye.");
                        query = false;
                        break;
                    default:
                        break;
                }

            }
            scanner.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
