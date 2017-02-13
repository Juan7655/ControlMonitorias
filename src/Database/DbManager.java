/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Database;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import sqldbconnect.DatabaseConnect;
import sqldbconnect.TipoDB;

/**
 *
 * @author juandavid
 */
public class DbManager extends DatabaseConnect{
    
    private static final DbManager INSTANCE = new DbManager();
    
    public static DbManager getInstance(){
        return INSTANCE;
    }
    
    private DbManager() {
        super("monitorias", "postgres", "pass", "localhost:5432", TipoDB.POSTGRES);
        super.openConnection();
        
        if(!super.isConnected())System.out.println("Connection not possible");
        super.closeConnection();
    }
    
    public HashMap<String, String> searchStudent(String code){
        String[] columns = new String[]{"estudiante_id", "nombre", "asignatura","profesor"};
        
        super.addSelect(true, columns, "estudiante", "estudiante_id = "+code, 
                null, null, null, null, null);
        super.openConnection();
        ResultSet tempResult = (ResultSet)super.execute(0);
        
        HashMap<String, String> result = new HashMap<>();
        
        try {
            while(tempResult.next()){
                result.put("estudiante_id", tempResult.getString(1));
                result.put("nombre", tempResult.getString(2));
                result.put("asignatura", tempResult.getString(3));
                result.put("profesor", tempResult.getString(4));
            }
        } catch (SQLException ex) {
            Logger.getLogger(DbManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        super.closeConnection();
        return result;
    }
    
    public String searchSimple(String table, String name){
        String[] columns = new String[]{table +"_id", "nombre"};
        String result = null;
        
        super.addSelect(true, columns, table, "nombre = '"+name+"'", 
                null, null, null, null, null);
        super.openConnection();
        ResultSet tempResult = (ResultSet)super.execute(0);
        
        try {
            while(tempResult.next()) result = tempResult.getString(table+"_id");
        } catch (SQLException ex) {
            Logger.getLogger(DbManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        super.closeConnection();
        return result;
    }
    
    public ArrayList searchArray(String table){
        String[] columns = new String[]{table +"_id", "nombre"};
        ArrayList<String> result = new ArrayList<>();
        
        super.addSelect(true, columns, table, null, 
                null, null, null, null, null);
        super.openConnection();
        ResultSet tempResult = (ResultSet)super.execute(0);
        
        try {
            while(tempResult.next()) result.add(tempResult.getString("nombre"));
        } catch (SQLException ex) {
            Logger.getLogger(DbManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        super.closeConnection();
        return result;
    }
    
    public void insert(String table, HashMap<String,String> values){
        super.addInsert(table, values, false);
        super.openConnection();
        super.executeAll();
        super.closeConnection();
    }
    
    
}
