import java.sql.*;
import java.util.ArrayList;

public class Queries {
    
    public static void searchStudentsByName(String name, String URL, String username, String password) throws SQLException {
        Connection conn = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            // conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/cs336","root", "");
            conn = DriverManager.getConnection("jdbc:mysql://" + URL, username, password);

            PreparedStatement ps = conn.prepareStatement("SELECT id FROM students WHERE first_name LIKE ? OR last_name LIKE ?");
            ps.setString(1, '%' + name + '%');
            ps.setString(2, '%' + name + '%');
            ResultSet rs = ps.executeQuery();
            allStudentMatches(rs);
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
        catch (IllegalArgumentException iae){
            System.out.println(iae.getMessage());
        }
        catch (RuntimeException re){
            System.out.println(re.getMessage());
        }
        finally {
            if (conn != null)
                conn.close();
        }
    }

    public static void searchStudentsByYear(String year, String URL, String username, String password) throws SQLException {
        Connection conn = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            //conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/cs336","root", "");
            conn = DriverManager.getConnection("jdbc:mysql://" + URL, username, password);

            year = year.toLowerCase();
            int lowerBound = 0;
            int higherBound = 0;

            switch(year){
                case "fr":
                    lowerBound = 0;
                    higherBound = 29;
                    break;
                case "so":
                    lowerBound = 30;
                    higherBound = 59;
                    break;
                case "ju":
                    lowerBound = 60;
                    higherBound = 89;
                    break;
                case "sr":
                    lowerBound = 90;
                    higherBound = 120;
                    break;
                default:
                    break;
            }
            
            PreparedStatement ps = conn.prepareStatement("SELECT SUM(classes.credits), id FROM students JOIN hasTaken JOIN classes " +
                                                         "ON students.id = hasTaken.sid AND hasTaken.name  = classes.name GROUP BY id " +
                                                         "HAVING SUM(classes.credits) >= ? AND SUM(classes.credits) <= ?");
            ps.setInt(1, lowerBound);
            ps.setInt(2, higherBound);
            ResultSet rs = ps.executeQuery();
            allStudentMatches(rs);
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
        catch (IllegalArgumentException iae){
            System.out.println(iae.getMessage());
        }
        catch (RuntimeException re){
            System.out.println(re.getMessage());
        }
        finally {
            if (conn != null)
                conn.close();
        }
    }

    public static void searchStudentsAboveGPA(double gpa, String URL, String username, String password) throws SQLException {
        Connection conn = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            //conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/cs336","root", "");
            conn = DriverManager.getConnection("jdbc:mysql://" + URL, username, password);

            PreparedStatement ps = conn.prepareStatement("SELECT students.id, SUM(CASE " +
                                        "WHEN HasTaken.grade = 'A' THEN 4.0 WHEN HasTaken.grade = 'B' THEN 3.0 " + 
                                        "WHEN HasTaken.grade = 'C' THEN 2.0 WHEN HasTaken.grade = 'D' THEN 1.0 ELSE 0.0 " + 
                                        "END * classes.credits) / SUM(classes.credits) AS gpa FROM students " + 
                                        "JOIN hasTaken ON students.id = hasTaken.sid JOIN classes ON hasTaken.name = classes.name " + 
                                        "GROUP BY students.id, students.first_name, students.last_name HAVING gpa >= ? ORDER BY gpa ASC");
            ps.setDouble(1, gpa);
            ResultSet rs = ps.executeQuery();
            allStudentMatches(rs);
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
        catch (IllegalArgumentException iae){
            System.out.println(iae.getMessage());
        }
        catch (RuntimeException re){
            System.out.println(re.getMessage());
        }
        finally {
            if (conn != null)
                conn.close();
        }
    }

    public static void searchStudentsBelowGPA(double gpa, String URL, String username, String password) throws SQLException {
        Connection conn = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            //conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/cs336","root", "");
            conn = DriverManager.getConnection("jdbc:mysql://" + URL, username, password);

            PreparedStatement ps = conn.prepareStatement("SELECT students.id, SUM(CASE " +
                                        "WHEN HasTaken.grade = 'A' THEN 4.0 WHEN HasTaken.grade = 'B' THEN 3.0 " + 
                                        "WHEN HasTaken.grade = 'C' THEN 2.0 WHEN HasTaken.grade = 'D' THEN 1.0 ELSE 0.0 " + 
                                        "END * classes.credits) / SUM(classes.credits) AS gpa FROM students " + 
                                        "JOIN hasTaken ON students.id = hasTaken.sid JOIN classes ON hasTaken.name = classes.name " + 
                                        "GROUP BY students.id, students.first_name, students.last_name HAVING gpa <= ? ORDER BY gpa ASC");
            ps.setDouble(1, gpa);
            ResultSet rs = ps.executeQuery();
            allStudentMatches(rs);
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
        catch (IllegalArgumentException iae){
            System.out.println(iae.getMessage());
        }
        catch (RuntimeException re){
            System.out.println(re.getMessage());
        }
        finally {
            if (conn != null)
                conn.close();
        }
    }

    public static void searchAverageDepartmentGPA(String dept, String URL, String username, String password) throws SQLException {
        Connection conn = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            //conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/cs336","root", "");
            conn = DriverManager.getConnection("jdbc:mysql://" + URL, username, password);

            PreparedStatement ps = conn.prepareStatement("SELECT COUNT(DISTINCT id), AVG(gpa) as avg_gpa " + 
            "FROM (SELECT students.id, majors.dname, " + 
                     "SUM(CASE " + 
                          "WHEN HasTaken.grade = 'A' THEN 4.0 " + 
                          "WHEN HasTaken.grade = 'B' THEN 3.0 " +
                          "WHEN HasTaken.grade = 'C' THEN 2.0 " + 
                          "WHEN HasTaken.grade = 'D' THEN 1.0 " +
                          "ELSE 0.0 " +
                        "END * classes.credits) / SUM(classes.credits) AS gpa " + 
              "FROM students JOIN hasTaken ON students.id = hasTaken.sid " + 
              "JOIN classes ON hasTaken.name = Classes.name " + 
              "JOIN majors ON students.id = majors.sid " + 
              "WHERE majors.dname = ? " + 
              "GROUP BY hasTaken.sid " + 
              "UNION ALL " + 
              "SELECT students.id, minors.dname, " + 
                     "SUM(CASE " +
                          "WHEN HasTaken.grade = 'A' THEN 4.0 " +
                          "WHEN HasTaken.grade = 'B' THEN 3.0 " +
                          "WHEN HasTaken.grade = 'C' THEN 2.0 " +
                          "WHEN HasTaken.grade = 'D' THEN 1.0 " +
                          "ELSE 0.0 " +
                        "END * classes.credits) / SUM(classes.credits) AS gpa " +
              "FROM students JOIN hasTaken ON students.id = hasTaken.sid " +
              "JOIN classes ON hasTaken.name = Classes.name " +
              "JOIN minors ON students.id = minors.sid " +
              "WHERE minors.dname = ? " +
              "GROUP BY hasTaken.sid " +
            ") as avgDeptGPAs");

            ps.setString(1, dept);
            ps.setString(2, dept);
            ResultSet rs = ps.executeQuery();
            
            while(rs.next()) {
                System.out.println("Num students: " + rs.getInt(1));
                System.out.println("Average GPA: " + rs.getDouble(2));
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
        catch (IllegalArgumentException iae){
            System.out.println(iae.getMessage());
        }
        catch (RuntimeException re){
            System.out.println(re.getMessage());
        }
        finally {
            if (conn != null)
                conn.close();
        }
    }

    public static void searchClass(String className, String URL, String username, String password) throws SQLException {
        Connection conn = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            //conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/cs336","root", "");
            conn = DriverManager.getConnection("jdbc:mysql://" + URL, username, password);

            PreparedStatement ps = conn.prepareStatement("SELECT COUNT(DISTINCT students.id) " + 
                                                         "FROM students JOIN isTaking ON students.id = isTaking.sid " + 
                                                         "WHERE isTaking.name = ?");
            ps.setString(1, className);
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                System.out.println(rs.getInt(1) + " students currently enrolled");
            }

            ps = conn.prepareStatement("SELECT " +
                                            "SUM(CASE WHEN hasTaken.grade = 'A' THEN 1 ELSE 0 END) AS a_grades, " + 
                                            "SUM(CASE WHEN hasTaken.grade = 'B' THEN 1 ELSE 0 END) AS b_grades, " +
                                            "SUM(CASE WHEN hasTaken.grade = 'C' THEN 1 ELSE 0 END) AS c_grades, " +
                                            "SUM(CASE WHEN hasTaken.grade = 'D' THEN 1 ELSE 0 END) AS d_grades, " +
                                            "SUM(CASE WHEN hasTaken.grade = 'F' THEN 1 ELSE 0 END) AS f_grades " +
                                        "FROM hasTaken WHERE hasTaken.name = ?");
            ps.setString(1, className);
            rs = ps.executeQuery();
            while(rs.next()){
                System.out.println("Grades of previous enrollees: ");
                System.out.println("A " + rs.getInt("a_grades"));
                System.out.println("B " + rs.getInt("b_grades"));
                System.out.println("C " + rs.getInt("c_grades"));
                System.out.println("D " + rs.getInt("d_grades"));
                System.out.println("F " + rs.getInt("f_grades"));
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
        catch (IllegalArgumentException iae){
            System.out.println(iae.getMessage());
        }
        catch (RuntimeException re){
            System.out.println(re.getMessage());
        }
        finally {
            if (conn != null)
                conn.close();
        }
    }

    public static void executeQuery(String sql, String URL, String username, String password) throws SQLException {
        Connection conn = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            //conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/cs336","root", "");
            conn = DriverManager.getConnection("jdbc:mysql://" + URL, username, password);

            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int numberOfColumns = rsmd.getColumnCount();
            String[] columns = new String[numberOfColumns];

            for(int i = 1; i <= numberOfColumns; i++){
                String col = rsmd.getColumnName(i);
                System.out.print(col + "\t");
                columns[i-1] = col;
            }
            System.out.println();

            while(rs.next()){
                for(String col: columns)
                    System.out.print(rs.getString(col) + "\t");
                System.out.print("\n");
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
        catch (IllegalArgumentException iae){
            System.out.println(iae.getMessage());
        }
        catch (RuntimeException re){
            System.out.println(re.getMessage());
        }
        finally {
            if (conn != null)
                conn.close();
        }
    }

    private static String studentMatch(int id) throws SQLException{
        Connection conn = null;
        String match = "";
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/cs336","root", "");

            PreparedStatement ps = conn.prepareStatement("SELECT students.first_name, students.last_name " + 
                                                        "FROM students " + 
                                                        "WHERE students.id = ?");
            ps.setInt(1, id);
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                match += rs.getString(2) + ", " + rs.getString(1) + "\n";
            }
            match += "ID: " + id + "\n";

            ps = conn.prepareStatement("SELECT dname FROM majors WHERE majors.sid = ?");
            ps.setInt(1, id);
            rs = ps.executeQuery();
            ArrayList<String> majors = new ArrayList<String>();
            while(rs.next()){
                majors.add(rs.getString(1));
            }
            if(majors.size() > 0){
                if(majors.size() == 1)
                    match += "Major: ";
                else   
                    match += "Majors: ";
                for(int i = 0; i < majors.size(); i++){
                    if(i != majors.size() - 1)
                        match += majors.get(i) + ", ";
                    else 
                        match += majors.get(i) + "\n";
                }
                   
            }
        
            ps = conn.prepareStatement("SELECT dname FROM minors WHERE minors.sid = ?");
            ps.setInt(1, id);
            rs = ps.executeQuery();
            ArrayList<String> minors = new ArrayList<String>();
            while(rs.next()){
                minors.add(rs.getString(1));
            }
            if(minors.size() > 0){
                if(minors.size() == 1)
                    match += "Minor: ";
                else   
                    match += "Minors: ";
                for(int i = 0; i < minors.size(); i++){
                    if(i != minors.size() - 1)
                        match += minors.get(i) + ", ";
                    else 
                        match += minors.get(i) + "\n";
                }
                   
            }

            ps = conn.prepareStatement("SELECT SUM(classes.credits), " + 
                                        "SUM(CASE WHEN HasTaken.grade = 'A' THEN 4.0 " + 
                                        "WHEN HasTaken.grade = 'B' THEN 3.0 " + 
                                        "WHEN HasTaken.grade = 'C' THEN 2.0 " + 
                                        "WHEN HasTaken.grade = 'D' THEN 1.0 " + 
                                        "ELSE 0.0 END * classes.credits) / SUM(classes.credits) AS gpa " + 
                                        "FROM students JOIN hasTaken ON students.id = hasTaken.sid " + 
                                        "JOIN classes ON hasTaken.name = classes.name " + 
                                        "WHERE students.id = ?");
            ps.setInt(1, id); 
            rs = ps.executeQuery();
            while(rs.next())
                match += "GPA: " + rs.getDouble(2) + "\nCredits: " + rs.getInt(1);
            
            return match;
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
        catch (IllegalArgumentException iae){
            System.out.println(iae.getMessage());
        }
        catch (RuntimeException re){
            System.out.println(re.getMessage());
        }
        finally {
            if(conn != null)
                conn.close();
        }
        return match;
    }

    private static void allStudentMatches(ResultSet rs) throws SQLException {
        try { 
            ArrayList<String> matches = new ArrayList<String>();
            while(rs.next()) 
                matches.add(studentMatch(rs.getInt("id")));
            System.out.println(matches.size() + " students found");
            for(String match: matches){
                System.out.println(match);
                System.out.println();
            }
        } catch (SQLException e){
            e.printStackTrace();
        }   
    }
}