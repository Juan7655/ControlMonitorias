/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Database;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author JuanDavid
 */
public class DBExtended extends Database {

    public DBExtended() {
        super("monitorias", "root", "123", "127.0.0.1:3306", "mysql");
    }
    
    public ArrayList<String> search(String table, String[] columns, String condicionante, String condicion){
        ArrayList<String> arr = new ArrayList<>();
        String cond=null;
        try {
            if(condicion!=null && condicionante!=null)cond = condicionante+"="+condicion;
            ResultSet result = (ResultSet)super.query(false, table, columns, cond, null, null, null, null, null);
            while(result.next())
                for(int i = 1; i < columns.length + 1; i++)arr.add(result.getString(i));
        } catch (Exception ex) {
            Logger.getLogger(DBExtended.class.getName()).log(Level.SEVERE, null, ex);
        }
        return arr;
    }
    
    @Override
    public void insert(String table, HashMap<String, String> values){
        try {
            super.insert(table, values);
            super.executeStatement(null, true);
        } catch (Exception ex) {
            Logger.getLogger(DBExtended.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}