package Database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author JuanDavid
 */
public abstract class Database {

    
    private final String username;
    private final String password;
    private final String url;
    private Connection conn;
    private String sqlCode = "";

    /**
     * @param DBName   Database schema name
     * @param username username for connection
     * @param password password for connecion
     * @param url      the url of access to de Database
     * @param client
     */
    protected Database(String DBName, String username, String password, String url, String client) {
        this.username = username;
        this.password = password;
        String extra;
        switch(client){
            case "mysql":extra = "?autoReconnect=true&useSSL=false";
            break;
            case "sqlserver":extra = ";integratedSecurity=true";
            break;
            case "db2":extra = ":retrieveMessagesFromServerOnGetMessage=true;";
            break;
            default:extra = "";
        }
        this.url = "jdbc:" + client + "://" + url + ((client.equals("sqlserver"))?";databaseName=":"/") + DBName + extra;
    }

    /**
     * Base method to create a connection with the Database Server. Must be called when querying
     */
    private void getConnection() {
        try {
            if(conn != null)if (!conn.isClosed())return;
            Class.forName("com.mysql.jdbc.Driver");
            //Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            //Class.forName("com.ibm.db2.jcc.DB2Driver");
            conn = DriverManager.getConnection(url, username, password);
            System.out.println("Connected succesfully!");
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println("Not connected: " + e);
        }
    }

    /**
     * @param table  Name of the table to insert
     * @param values Hashmap with a relation between key(column name) and value (value it will take).
     * @throws Exception
     */
    protected void insert(String table, HashMap<String, String> values) throws Exception {
        if (table == null) throw new Exception("El nombre de la tabla no puede estar vacío");
        String insertBody = putBody(values);
        this.sqlCode += " INSERT INTO " + table + " SET " + insertBody + ";";
    }

    /**
     * @param table       Name of the table to delete from
     * @param whereClause The sql sentence corresponding but not including the WHERE section
     * @param whereArgs   An array of values that will be put instead of the corresponding ? symbols in the WHERE clause
     * @throws Exception
     */
    protected void delete(String table, String whereClause, String[] whereArgs) throws Exception {
        if (table == null) throw new Exception("El nombre de la tabla no puede estar vacío");
        this.sqlCode += "DELETE FROM " + table + putWhereArgs(whereClause, whereArgs) + ";";
    }

    /**
     * @param distinct    True if only different values shall be retreated. False to ignore this section.
     * @param table       Name of the table to read from.
     * @param columns     Columns to SELECT.
     * @param whereClause The sql sentence corresponding but not including the WHERE section.
     * @param whereArgs   An array of values that will be put instead of the corresponding ? symbols in the WHERE clause.
     * @param groupBy     The value corresponding to the GROUP BY clause. Null to ignore.
     * @param having      The value corresponding to the HAVING clause. Null to ignore.
     * @param orderBy     The value corresponding to the ORDER BY clause. Null to ignore.
     * @param limit       The value corresponding to the LIMIT clause. Null to ignore. Limits to a fixed number the results.
     * @return Return an object of type ResultSet with the result of the query.
     * @throws Exception
     */
    protected Object query(boolean distinct, String table, String[] columns, String whereClause, String[] whereArgs,
                              String groupBy, String having, String orderBy, String limit) throws Exception {
        if (table == null) throw new Exception("El nombre de la tabla no puede estar vacío");

        return this.executeStatement("SELECT " + ((distinct) ? "DISTINCT " : "") + selectCol(columns)
                + " FROM " + table + putWhereArgs(whereClause, whereArgs) + queringParams(groupBy, having, orderBy, limit) + ";", false);
        }

    /**
     * @param table       Name of the table to execute the update.
     * @param values      Hashmap with a relation between key(column name) and value (value it will take).
     * @param whereClause The sql sentence corresponding but not including the WHERE section.
     * @param whereArgs   An array of values that will be put instead of the corresponding ? symbols in the WHERE clause.
     * @throws Exception
     */
    protected void update(String table, HashMap<String, String> values, String whereClause, String[] whereArgs) throws Exception {
        if (table == null) throw new Exception("El nombre de la tabla no puede estar vacío");
        this.sqlCode += "UPDATE " + table + " SET " + putBody(values) + putWhereArgs(whereClause, whereArgs) + ";";
    }

    private String putWhereArgs(String whereClause, String[] whereArgs) throws Exception {
        String temp = ((whereClause != null) ? whereClause.replace("?", "") : "");
        if (whereArgs != null && whereClause != null) {
            if ((whereClause.length() - temp.length()) != whereArgs.length) throw new Exception("Could not match Where Arguments with '?' characters in the input");
            for (String i : whereArgs)whereClause = whereClause.replaceFirst("\\?", i);
        }
        return ((whereClause != null) ? " WHERE " + whereClause : "");
    }

    private String putBody(HashMap<String, String> values) {
        String insertBody = "";
        for(Map.Entry entry : values.entrySet())
            insertBody += ((insertBody.equals("")) ? "" : ", ") + entry.getKey() + "=" + entry.getValue();
        return insertBody;
    }

    private String selectCol(String[] columns) {
        if (columns == null)columns = new String[]{"*"};
        String select = "";
        for (String i : columns)select += (!select.equals("")?", ":"")+i;
        return select;
    }

    private String queringParams(String groupBy, String having, String orderBy, String limit) {
        String state = "";
        if(groupBy != null)state+=" GROUP BY " + groupBy;
        if(having != null)state+=" HAVING " + having;
        if(orderBy != null)state+=" ORDER BY " + orderBy;
        if(limit != null)state+=" LIMIT " + limit;

        return state;
    }

    protected Object executeStatement(String sql, Boolean update) {
        getConnection();
        try {
            if (update) {
                String[] sqlStatements = this.sqlCode.split(";");
                int r=0;
                System.out.println("Executing "+sqlStatements.length);
                for(String i:sqlStatements)r+=conn.prepareStatement(i).executeUpdate();
                System.out.println("Executed succesfully ");
                this.close();
                this.sqlCode="";
                return r;
            } else return conn.prepareStatement(sql).executeQuery();
           } catch (SQLException e) {
               System.out.println("Error: "+e);
            return ((update) ? -1 : null);
        }
    }
    
    protected void executeStatement() {
        getConnection();
        try {
            for(int i = 16; i < 21; i++) conn.prepareStatement("INSERT INTO tbl1(nombre, descripcion) VALUES ("+i+", 'PRIMERA PRUEBA NETBEANS"+i+"')");
            conn.prepareStatement("INSERT INTO tbl1(nombre, descripcion) VALUES ('563', 'PRIMERA PRUEBA NETBEANS 151')").executeUpdate();
            
              System.out.println("Committed succesfully :)");  
           } catch (SQLException e) {
               System.out.println("Error: "+e);
        }
    }

    /**
     * Method to close the connection with the Database. DO NOT LEAVE THE CONNECTION OPENED.
     */
    public void close() {
        if (conn == null) return;
        try {
        if (conn.isClosed()) return;
            conn.close();
            System.out.println("Connection Closed");
        } catch (SQLException e) {
            System.out.println("Error: " + e);
        }
    }
}