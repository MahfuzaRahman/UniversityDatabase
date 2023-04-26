import java.io.*;
import java.sql.*;
import java.util.*;

public class App {
    // public static void main(String[] args) throws ClassNotFoundException, SQLException {
    //     Connection conn = null;
    //     try {
    //         Class.forName("com.mysql.cj.jdbc.Driver");
    //         conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/cs336","root", "");
    //         insertDepartmentsData(conn);
    //         insertStudentsData(conn);
    //         insertClassesData(conn);
    //         insertMajorsData(conn);
    //         insertMinorsData(conn);
    //         insertIsTakingData(conn);
    //         insertHasTakenData(conn);
    //     }
    //     catch (SQLException e){
    //         e.printStackTrace();
    //     }
    //     catch (ClassNotFoundException e){
    //         e.printStackTrace();
    //     }
    //     catch (IllegalArgumentException iae){
    //         System.out.println(iae.getMessage());
    //     }
    //     catch (RuntimeException re){
    //         System.out.println(re.getMessage());
    //     }
    //     finally {
    //         if (conn != null)
    //             conn.close();
    //     }
    // }

    public static void insertDepartmentsData(Connection conn) throws ClassNotFoundException, SQLException {
        try {
            PreparedStatement ps = conn.prepareStatement("DROP TABLE IF EXISTS departments"); 
            ps.executeUpdate();
            ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS departments(name VARCHAR(4), campus VARCHAR(5), PRIMARY KEY (name))");
            ps.executeUpdate();
            ArrayList<String> names = new ArrayList<String>(Arrays.asList("Bio","Chem", "CS", "Eng", "Math", "Phys"));
            ArrayList<String> campus = new ArrayList<String>(Arrays.asList("Busch", "CAC", "Busch", "Livi", "CD", "Livi"));
            int n = names.size();
            ps = conn.prepareStatement("INSERT INTO departments VALUES(?, ?)");
            
            for (int i = 0; i < n; i++){
                ps.setString(1, names.get(i));
                ps.setString(2, campus.get(i));
                ps.executeUpdate();
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void insertStudentsData(Connection conn) throws ClassNotFoundException, SQLException {
        try {
            PreparedStatement ps = conn.prepareStatement("DROP TABLE IF EXISTS students"); 
            ps.executeUpdate();
            ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS students(first_name VARCHAR(50), last_name VARCHAR(50), id INT, PRIMARY KEY (id))");
            ps.executeUpdate();
            
            FileInputStream fstream = new FileInputStream("src/input/students.txt");
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            ArrayList<String> list = new ArrayList<String>();
            
            while ((strLine = br.readLine()) != null){
                list.add(strLine);
            }
            br.close();
            
            Iterator itr;
            for (itr=list.iterator(); itr.hasNext(); ){
                String str = itr.next().toString();  
                String [] splitSt =str.split(",");
                String first_name = "", last_name = "";
                int id = 0;
                for (int i = 0 ; i < splitSt.length ; i++) {
                    first_name = splitSt[0];
                    last_name = splitSt[1];
                    id = Integer.parseInt(splitSt[2]);
                }
                ps = conn.prepareStatement("INSERT INTO students VALUES(?, ?, ?)");
                ps.setString(1, first_name);
                ps.setString(2, last_name);
                ps.setInt(3, id);
                ps.executeUpdate();
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void insertClassesData(Connection conn) throws ClassNotFoundException, SQLException {
        try {
            PreparedStatement ps = conn.prepareStatement("DROP TABLE IF EXISTS classes"); 
            ps.executeUpdate();
            ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS classes(name VARCHAR(50), credits INT, PRIMARY KEY (name))");
            ps.executeUpdate();
            ArrayList<String> names = new ArrayList<String>(Arrays.asList("Bio-101","Bio-201", "Bio-301", "Bio-401", "Bio-Lab", 
                                                                          "Chem-101","Chem-201", "Chem-301", "Chem-401", "Chem-Lab",
                                                                          "CS-101","Data-Structures", "CS-205", "CS-206", "CS-336", "CS-344",
                                                                          "Eng-101","Eng-201", "Eng-301", "Eng-401", "Eng-Lab",
                                                                          "Math-101","Math-201", "Math-301", "Math-401", "Math-501",
                                                                          "Phys-101","Phys-201", "Phys-301", "Phys-401", "Phys-Lab"));
            ArrayList<Integer> credits = new ArrayList<Integer>(Arrays.asList(3,3,3,4,4,3,4,3,4,3,3,3,4,3,3,3,4,4,4,4,3,4,3,3,3,4,4,3,3,4,4));
            int n = names.size();
            ps = conn.prepareStatement("INSERT INTO classes VALUES(?, ?)");
            for (int i = 0; i < n; i++){
                ps.setString(1, names.get(i));
                ps.setInt(2, credits.get(i));
                ps.executeUpdate();
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void insertMajorsData(Connection conn) throws ClassNotFoundException, SQLException {
        try {
            PreparedStatement ps = conn.prepareStatement("DROP TABLE IF EXISTS majors"); 
            ps.executeUpdate();
            ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS majors(sid INT, dname VARCHAR(4), FOREIGN KEY (sid) REFERENCES students(id), FOREIGN KEY (dname) REFERENCES departments(name))");
            ps.executeUpdate();
            
            FileInputStream fstream = new FileInputStream("src/input/students.txt");
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            ArrayList<String> list = new ArrayList<String>();
            
            while ((strLine = br.readLine()) != null){
                list.add(strLine);
            }
            br.close();
            
            Statement stmt = conn.createStatement();
            Iterator itr;
            for (itr=list.iterator(); itr.hasNext(); ){
                String str = itr.next().toString();  
                String [] splitSt =str.split(",");

                ps = conn.prepareStatement("INSERT INTO majors VALUES(?, ?)");
                int sid = Integer.parseInt(splitSt[2]);
                ResultSet rs = stmt.executeQuery("SELECT name FROM departments ORDER BY RAND() LIMIT 1");
                rs.next();
                String dname = rs.getString(1);
                ps.setInt(1, sid);
                ps.setString(2, dname);
                ps.executeUpdate();

                boolean doubleMajor = Math.random() > 0.85;
                if(doubleMajor){
                    rs = stmt.executeQuery("SELECT name FROM departments WHERE name <> '" + dname + "' ORDER BY RAND() LIMIT 1");
                    rs.next();
                    dname = rs.getString(1);
                    ps.setInt(1, sid);
                    ps.setString(2, dname);
                    ps.executeUpdate();
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void insertMinorsData(Connection conn) throws ClassNotFoundException, SQLException {
        try {
            PreparedStatement ps = conn.prepareStatement("DROP TABLE IF EXISTS minors"); 
            ps.executeUpdate();
            ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS minors(sid INT, dname VARCHAR(4), FOREIGN KEY (sid) REFERENCES students(id), FOREIGN KEY (dname) REFERENCES departments(name))");
            ps.executeUpdate();
            
            FileInputStream fstream = new FileInputStream("src/input/students.txt");
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            ArrayList<String> list = new ArrayList<String>();
            
            while ((strLine = br.readLine()) != null){
                list.add(strLine);
            }
            br.close();
            
            Statement stmt = conn.createStatement();
            Iterator itr;
            for (itr=list.iterator(); itr.hasNext(); ){
                String str = itr.next().toString();  
                String [] splitSt =str.split(",");

                ps = conn.prepareStatement("INSERT INTO minors VALUES(?, ?)");
                int sid = Integer.parseInt(splitSt[2]);
                ResultSet rs = stmt.executeQuery("SELECT name FROM departments WHERE name NOT IN (SELECT dname FROM MAJORS WHERE sid = " + sid + ") ORDER BY RAND()");
                rs.next();
                String dname = rs.getString(1);
                ps.setInt(1, sid);
                ps.setString(2, dname);
                ps.executeUpdate();

                boolean doubleMinor = Math.random() > 0.75;
                if(doubleMinor){
                    rs = stmt.executeQuery("SELECT name FROM departments WHERE name NOT IN (SELECT dname FROM MAJORS WHERE sid = " + sid + ")"
                                                                        + "AND name <> '" + dname + "' ORDER BY RAND()");
                    rs.next();
                    dname = rs.getString(1);
                    ps.setInt(1, sid);
                    ps.setString(2, dname);
                    ps.executeUpdate();
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void insertIsTakingData(Connection conn) throws ClassNotFoundException, SQLException {
        try {
            PreparedStatement ps = conn.prepareStatement("DROP TABLE IF EXISTS isTaking"); 
            ps.executeUpdate();
            ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS isTaking(sid INT, name VARCHAR(50), FOREIGN KEY (sid) REFERENCES students(id), FOREIGN KEY (name) REFERENCES classes(name))");
            ps.executeUpdate();
            
            FileInputStream fstream = new FileInputStream("src/input/students.txt");
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            ArrayList<String> list = new ArrayList<String>();
            
            while ((strLine = br.readLine()) != null){
                list.add(strLine);
            }
            br.close();
            
            Statement stmt = conn.createStatement();
            Iterator itr;
            for (itr=list.iterator(); itr.hasNext(); ){
                String str = itr.next().toString();  
                String [] splitSt =str.split(",");

                ps = conn.prepareStatement("INSERT INTO isTaking VALUES(?, ?)");
                int sid = Integer.parseInt(splitSt[2]);
                ResultSet rs = stmt.executeQuery("SELECT * FROM classes ORDER BY RAND() LIMIT 5");
                int credits = 0;
                
                while(credits < 15 && rs.next()){
                    String name = rs.getString("name");
                    ps.setInt(1, sid);
                    ps.setString(2, name);
                    credits += rs.getInt("credits");
                    ps.executeUpdate();
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void insertHasTakenData(Connection conn) throws ClassNotFoundException, SQLException {
        try {
            PreparedStatement ps = conn.prepareStatement("DROP TABLE IF EXISTS hasTaken"); 
            ps.executeUpdate();
            ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS hasTaken(sid INT, name VARCHAR(50), grade VARCHAR(1), FOREIGN KEY (sid) REFERENCES students(id), FOREIGN KEY (name) REFERENCES classes(name))");
            ps.executeUpdate();
            
            FileInputStream fstream = new FileInputStream("src/input/students.txt");
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            ArrayList<String> list = new ArrayList<String>();
            
            while ((strLine = br.readLine()) != null){
                list.add(strLine);
            }
            br.close();
            
            Statement stmt = conn.createStatement();
            Iterator itr;
            for (itr=list.iterator(); itr.hasNext(); ){
                String str = itr.next().toString();  
                String [] splitSt =str.split(",");

                ps = conn.prepareStatement("INSERT INTO hasTaken VALUES(?, ?, ?)");
                int sid = Integer.parseInt(splitSt[2]);
                ResultSet rs = stmt.executeQuery("SELECT * FROM classes WHERE name NOT IN (SELECT name FROM isTaking WHERE sid = " + sid + ") ORDER BY RAND()");
                int year = (int) (Math.random() * 120);
                int credits = 0;
                String[] grades = {"A", "B", "C", "D", "F"};
                
                while(credits < year && rs.next()){
                    String name = rs.getString("name");
                    ps.setInt(1, sid);
                    ps.setString(2, name);
                    int grade = (int) (Math.random() * 5);
                    ps.setString(3, grades[grade]);
                    credits += rs.getInt("credits");
                    ps.executeUpdate();
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}