package demo.extractor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import com.opencsv.CSVWriter;

public class ExtractData {
	

	public static Connection connnection;
	
//	public static String JDBC_URL = "jdbc:sqlserver://localhost:1433;databaseName=shifenzheng;integratedSecurity=true";
	
	public static String JDBC_URL = "jdbc:sqlserver://localhost:1433;databaseName=shifenzheng";
	
	public static void getDbConnection() {
		
		try {
			
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			
			connnection = DriverManager.getConnection(JDBC_URL,"sa","123");
			
			if(connnection != null) {
				
				DatabaseMetaData metaObj = (DatabaseMetaData) connnection.getMetaData();
				
//				System.out.println("Driver Name?= " + metaObj.getDriverName() + ", Driver Version?= " + metaObj.getDriverVersion() + ", Product Name?= " + metaObj.getDatabaseProductName() + ", Product Version?= " + metaObj.getDatabaseProductVersion());
				
			}
			
		} catch(Exception sqlException) {
			
			sqlException.printStackTrace();
			
		}
		
	}
	

	public static void main(String[] args) throws Exception{
		getDbConnection();
		Statement stmt = connnection.createStatement();
		ResultSet rs = stmt.executeQuery("select * from dbo.cdsgus");
		
		ResultSetMetaData metadata = rs.getMetaData();
		
		int columnCount = metadata.getColumnCount();
		String[] columnNames = new String[columnCount];
		for(int i=0;i<columnCount;i++){
			columnNames[i]=metadata.getColumnName(i+1);
		}
		
//		System.out.println(String.join(",", columnNames));
		
		File csv = new File("d:/cdsgus.csv");
		if (csv.exists()) {
			csv.delete();
		}
		
		try {
			csv.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		CSVWriter writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(csv), "UTF-8"),
		CSVWriter.DEFAULT_SEPARATOR, CSVWriter.DEFAULT_QUOTE_CHARACTER);
				
		
		System.out.println();
		
		writer.writeNext(columnNames);
		
		int idx = 0;
		while(rs.next()){
			//System.out.println(rs.getString(1));
			String[] content = new String[columnCount];
			int i=0;
			for(String colName:columnNames){
				
				content[i] = rs.getString(colName);
//				if(content[i]==null){
//					content[i]="";
//				}
				i++;
			}
			
//			System.out.println(String.join(",", content));
			writer.writeNext(content);
			idx++;
			if (idx%10000==0){
				writer.flush();
				System.out.println("fush to disk, idx = "+idx);
			}
		}
		System.out.println("total count is: "+ idx);
		rs.close();
		stmt.close();
		connnection.close();
		writer.flush();
		writer.close();

	}

}
