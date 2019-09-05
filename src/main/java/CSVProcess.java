
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class CSVProcess {
	
	/*
	 * Read file in to list, create the database and table, insert into table,
	 * create the csv with bad records, write statistics to log file
	 */
	
	public static void main(String[] args) {
		if(args.length == 1)
			processCSV(args[0]);
		else
			System.out.println("Error: Incorrect number of arguments");
	}

	private static void processCSV(String file) {
		
		//statistics for log file
		int recieved = 0;
		int successful = 0;
		int failed = 0;
		String warning = null;
		
		BufferedReader bfr = null;
		String row[] = null;
		ArrayList<String[]> goodRows = new ArrayList<String[]>();
		ArrayList<String[]> badRows = new ArrayList<String[]>();

		try {
			bfr = new BufferedReader(new FileReader(file));
			CSVReader cr = new CSVReaderBuilder(bfr).withSkipLines(1).build();
			while((row = cr.readNext()) != null) {
				recieved++;
				if(row.length == 10) {
					goodRows.add(row);
					successful++;
				} else {
					badRows.add(row);
					failed++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		Connection connection = getConnection(file);
		createTable(connection);
		insertRecords(connection, goodRows);
		
		
		if(!checkRowCount(connection, successful))
			warning = "There might be missing rows in the database";
		
		makeCSV(file, badRows);
		logToFile(recieved, successful, failed, warning, file);
		
		try {
			if(!connection.isClosed()) 
				connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	private static void logToFile(int recieved, int successful, int failed, String warning, String filename) {
		
		filename = filename.substring(0, filename.lastIndexOf('.'));
		filename = filename + ".log";
		BufferedWriter bfw = null;
		try {
			bfw = new BufferedWriter(new FileWriter(filename));
			bfw.flush();
			bfw.write("recieved:   " + recieved);
			bfw.newLine();
			bfw.write("successful: " + successful);
			bfw.newLine();
			bfw.write("failed:     " + failed);
			if(warning != null) {
				bfw.newLine();
				bfw.write(warning);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				bfw.close();
			}catch (IOException e){}
		}
		
	}

	private static void makeCSV(String filename, ArrayList<String[]> records) {
		
		filename = filename.substring(0, filename.lastIndexOf('.'));
		filename = filename + "-bad.csv";
		BufferedWriter bfw = null;
		StringBuilder sb = null;
		
		try {
			bfw = new BufferedWriter(new FileWriter(filename));
			bfw.flush();
			for(String[] row: records) {
				sb = new StringBuilder();
				for(String data: row) {
					sb.append(data);
					sb.append(",");
				}
				bfw.write(sb.toString());
				bfw.newLine();
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				bfw.close();
			}catch (IOException e){}
		}
	}
	
	private static void createTable(Connection connection) {
		
		String dropTableSQL = "drop table if exists csv_data;";
		String vacuumSQL = "VACUUM;";
		String createTableSQL = "create table csv_data (\n"
							  + " A text,\n"
							  + " B text,\n"
							  + " C text,\n"
							  + " D text,\n"
							  + " E text,\n"
							  + " F text,\n"
							  + " G text,\n"
							  + " H text,\n"
							  + " I text,\n"
							  + " J text\n"
							  + ");";
		
		try {
			Statement statement = connection.createStatement();
			statement.execute(dropTableSQL);
			statement.execute(vacuumSQL);
			statement.execute(createTableSQL);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	private static void insertRecords(Connection connection, ArrayList<String[]> records) {
		
		//optimized the insert by turning off autocommit and executing insert in batches
		
		String insertSQL = "insert into csv_data values (?,?,?,?,?,?,?,?,?,?);";
		final int batchSize = 1000;
		int count = 0;
		
		try {
			PreparedStatement statement = connection.prepareStatement(insertSQL);
			for(String[] row: records) {		
				statement.setString(1, row[0]);
				statement.setString(2, row[1]);
				statement.setString(3, row[2]);
				statement.setString(4, row[3]);
				statement.setString(5, row[4]);
				statement.setString(6, row[5]);
				statement.setString(7, row[6]);
				statement.setString(8, row[7]);
				statement.setString(9, row[8]);
				statement.setString(10, row[9]);
				statement.addBatch();
				if(++count % batchSize == 0){
					connection.setAutoCommit(false);
					statement.executeBatch();
					connection.setAutoCommit(true);
				}
			}
			connection.setAutoCommit(false);
			statement.executeBatch();
			connection.setAutoCommit(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
						  
	}
	
	private static boolean checkRowCount(Connection connection, int successful) {
		
		boolean success = false;
		String selectSQL = "select count(*) as numOfRows from csv_data;";
		try {
			Statement statement = connection.createStatement();
			ResultSet rs = statement.executeQuery(selectSQL);
			int count = rs.getInt("numOfRows");
			if(count == successful)
				success = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return success;
	}
	
	public static Connection getConnection(String filename) {
		Connection ctn = null;
		filename = filename.substring(0, filename.lastIndexOf('.'));
		String url = "jdbc:sqlite:" + filename + ".db";
		
		try {
			Class.forName("org.sqlite.JDBC");
			ctn = DriverManager.getConnection(url);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ctn;
	}
}
