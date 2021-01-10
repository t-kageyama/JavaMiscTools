package jp.co.comona.javamisc.sql;

import jp.co.comona.javamisc.Util;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * SQL record tool.
 * date: 2021/01/10
 * @author Toru Kageyama <info@comona.co.jp>
 */
abstract public class SQLRecord {

	// MARK: - Static Properties
	protected static final int SUCCESS_VALUE = 0;
	protected static final int ERROR_VALUE = -1;
	protected static final String SQL_SELECT_FROM = "SELECT * FROM ";
	protected static final String SQL_INSERT_INTO = "INSERT INTO ";
	protected static final String SQL_VALUES_START = " VALUES(";
	protected static final String SQL_DEFAULT = "DEFAULT";
	protected static final String SQL_NOW = "NOW()";
	protected static final String SQL_NULL = "NULL";
	protected static final String SQL_COMMA = ", ";
	protected static final char SQL_PREPARED_MARK = '?';
	protected static final char SQL_VALUES_END = ')';

	// MARK: - Properties
	final protected Options options;
	final protected CommandLine cmd;
	protected Connection con = null;
	protected ResultSetMetaData meta = null;
	protected String databaseName = null;
	protected String tableName = null;
	protected String hostName = "localhost";
	protected String userName = null;
	protected String password = null;
	protected String[] columns = null;
	protected String[] replaces = null;
	protected String[] nowColumns = null;
	protected String[] nulls = null;
	protected boolean prompt = false;

	// MARK: - Constructor
	/**
	 * constructor.
	 * @param options command line options.
	 * @param cmd command line.
	 */
	protected SQLRecord(Options options, CommandLine cmd) {
		super();
		this.options = options;
		this.cmd = cmd;
	}

	// MARK: - SQL
	/**
	 * connect to database.
	 * @return true if connected.
	 * @throws ClassNotFoundException when driver load error.
	 * @throws SQLException when SQL connection error.
	 */
	protected boolean connect() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");	// load MySQL database driver.

		String url =  "jdbc:mysql://" + hostName + ":3306/" + databaseName + "?useUnicode=true&autoReconnect=true&characterEncoding=utf8&useSSL=false";
		if (prompt) {
			// try 3 times.
			for (int i = 0; i < 3; i++) {
				try {
					doPrompt(url);
				}
				catch (Exception ignored) {}
				if (con != null) {
					break;
				}
			}
			if (con == null) {
				System.out.println("Giving up to connect...");
			}
		}
		else if (password != null) {
			con = DriverManager.getConnection(url, userName, password);
		}

		return con != null;
	}

	/**
	 * do prompt.
	 * @param url connection URL.
	 * @throws IOException when read terminal error.
	 * @throws SQLException when SQL connection error.
	 */
	private void doPrompt(String url) throws IOException, SQLException {
		Console console = System.console();
		if (console != null) {
			char[] passwordArray = console.readPassword("Enter password for user " + userName + ": ");
			password = new String(passwordArray);
		}
		else {
			System.out.print("Enter password for user " + userName + ": ");
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			password = br.readLine();
		}
		con = DriverManager.getConnection(url, userName, password);
	}

	/**
	 * disconnect from database.
	 * @throws SQLException when SQL disconnection error.
	 */
	protected void disconnect() throws SQLException {
		if (con != null) {
			con.close();
		}
	}

	/**
	 * load metadata.
	 * @throws SQLException when SQL error.
	 */
	protected void loadMetadata() throws SQLException {
		try (Statement stmt = con.createStatement(); ResultSet rs = stmt.executeQuery(SQL_SELECT_FROM + tableName + " LIMIT 1 OFFSET 0")) {
			meta = rs.getMetaData();
		}
	}

	/**
	 * convert string to date.
	 * @param value date value in string.
	 * @return date.
	 * @throws java.text.ParseException when parser error.
	 */
	protected static Date convertDate(String value) throws ParseException {
		try {
			SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy-MM-dd");
			return new Date(sdFormat.parse(value).getTime());
		}
		catch (Exception e) {
			return convertDateTime(value);
		}
	}

	/**
	 * convert string to date.
	 * @param value date value in string.
	 * @return date.
	 * @throws java.text.ParseException when parser error.
	 */
	protected static Date convertDateTime(String value) throws ParseException {
		SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		return new Date(sdFormat.parse(value).getTime());
	}

	/**
	 * convert string to date.
	 * @param value date value in string.
	 * @return date.
	 * @throws java.text.ParseException when parser error.
	 */
	protected static Time convertTime(String value) throws ParseException {
		SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		return new Time(sdFormat.parse("1970-01-01 " + value).getTime());
	}

	/**
	 * convert string to date.
	 * @param value date value in string.
	 * @return date.
	 * @throws java.text.ParseException when parser error.
	 */
	protected static Timestamp convertTimestamp(String value) throws ParseException {
		SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		return new Timestamp(sdFormat.parse(value).getTime());
	}

	/**
	 * put prepare statement index to column.
	 * @param columnIndexMap column to prepared statement index map.
	 * @param colName column name.
	 * @param columnIndex column prepared statement index.
	 * @return true if index put to the map.
	 */
	protected boolean putColumnIndex(Map<String, Integer> columnIndexMap, String colName, int columnIndex) {
		for (String column : columns) {
			if (column.compareToIgnoreCase(colName) == 0) {
				columnIndexMap.put(column, columnIndex);
				return true;
			}
		}
		return false;
	}

	/**
	 * put column value to prepared statement.
	 * @param ps prepared statement.
	 * @param columnIndexMap column to prepared statement index map.
	 * @param colName column name.
	 * @param columnType column type.
	 * @return true if value put to the prepared statement.
	 * @throws Exception when error.
	 */
	protected boolean putColumnValue(PreparedStatement ps, Map<String, Integer> columnIndexMap, String colName, int columnType) throws Exception {
		for (int i = 0; i < columns.length; i++) {
			String column = columns[i];
			if (column.compareToIgnoreCase(colName) == 0) {
				Integer indexObj = columnIndexMap.get(column);
				if (indexObj != null) {
					String value = replaces[i];
					int index = indexObj;
					switch (columnType) {
						case Types.INTEGER:
							ps.setInt(index, Integer.parseInt(value));
							break;
						case Types.BIGINT:
						case Types.DECIMAL:
							ps.setLong(index, Long.parseLong(value));
							break;
						case Types.SMALLINT:
							ps.setShort(index, Short.parseShort(value));
							break;
						case Types.TINYINT:
							short v = Short.parseShort(value);
							if ((v > 127) || (v < -128)) {
								throw new Exception("TINYINT out of range at " + colName);
							}
							ps.setInt(index, v);
							break;
						case Types.FLOAT:
							ps.setFloat(index, Float.parseFloat(value));
							break;
						case Types.DOUBLE:
						case Types.NUMERIC:
							ps.setDouble(index, Double.parseDouble(value));
							break;
						case Types.DATE:
							ps.setDate(index, convertDate(value));
							break;
						case Types.TIMESTAMP:
							ps.setTimestamp(index, convertTimestamp(value));
							break;
						case Types.TIME:
							ps.setTime(index, convertTime(value));
							break;

						default:
							ps.setString(index, value);
							break;
					}
				}
				return indexObj != null;
			}
		}
		return false;
	}

	// MARK: - Process
	/**
	 * do process.
	 * @throws Exception when error.
	 */
	protected abstract void doProcess() throws Exception;

	/**
	 * connect and process.
	 * @throws Exception when error.
	 */
	protected void connectAndProcess() throws Exception {
		if (connect()) {
			loadMetadata();
			doProcess();
			disconnect();
		}
	}

	// MARK: - Check
	/**
	 * check arguments.
	 * @return 0 if success, positive if help, negative if error.
	 */
	protected abstract int checkArguments();

	/**
	 * is now value column.
	 * @param columnIndex column index.
	 * @return true if use NOW().
	 * @throws SQLException when SQL error.
	 */
	protected boolean isNowValueColumn(int columnIndex) throws SQLException {
		if (nowColumns != null) {
			for (String now : nowColumns) {
				if (now.compareToIgnoreCase(meta.getColumnName(columnIndex)) == 0) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * is null value column.
	 * @param columnIndex column index.
	 * @return true if use null.
	 * @throws SQLException when SQL error.
	 */
	protected boolean isNullValueColumn(int columnIndex) throws SQLException {
		if (nulls != null) {
			for (String nullCol : nulls) {
				if (nullCol.compareToIgnoreCase(meta.getColumnName(columnIndex)) == 0) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * check database name.
	 * @return true if success.
	 */
	protected boolean checkDatabaseName() {
		String[] databases = cmd.getOptionValues('d');	// database name check.
		if (databases.length > 1) {
			usage(options);
			noMultipleOptions("database name");
			return false;
		}
		databaseName = databases[0];
		return true;
	}

	/**
	 * check table name.
	 * @return true if success.
	 */
	protected boolean checkTableName() {
		String[] tables = cmd.getOptionValues('t');	// table name check.
		if (tables.length > 1) {
			usage(options);
			noMultipleOptions("table name");
			return false;
		}
		tableName = tables[0];
		return true;
	}

	/**
	 * check host name.
	 * @return true if success.
	 */
	protected boolean checkHostName() {
		String[] hosts = cmd.getOptionValues('h');	// host name check.
		if (hosts != null) {
			if (hosts.length > 1) {
				usage(options);
				noMultipleOptions("host name");
				return false;
			}
			hostName = hosts[0];
		}
		return true;
	}

	/**
	 * check user name.
	 * @return true if success.
	 */
	protected boolean checkUserName() {
		String[] users = cmd.getOptionValues('u');	// user name check.
		if (users.length > 1) {
			usage(options);
			noMultipleOptions("user name");
			return false;
		}
		userName = users[0];
		return true;
	}

	/**
	 * check user password.
	 * @return true if success.
	 */
	protected boolean checkUserPassword() {
		prompt = cmd.hasOption('p');
		if (prompt && cmd.hasOption('P')) {	// password & prompt option check.
			usage(options);
			doNotAssignPromptAndPasswordAtSameTime();
			return false;
		}
		String[] passwords = cmd.getOptionValues('P');	// password check.
		if (passwords != null) {
			if (passwords.length > 1) {
				usage(options);
				noMultipleOptions("user password");
				return false;
			}
			password = passwords[0];
		}
		return true;
	}

	/**
	 * check columns & replaces.
	 * @return true if success.
	 */
	protected boolean checkColumnsAndReplaces() {
		columns = cmd.getOptionValues('c');	// column & replace count check.
		if (Util.hasDuplicateValuesIgnoreCase(columns)) {
			usage(options);
			duplicateValueFound("column name to replace value");
			return false;
		}
		replaces = cmd.getOptionValues(replaceValueShortOption());
		if (columns.length != replaces.length) {
			usage(options);
			columnAndReplaceCountMustSame();
			return false;
		}
		return true;
	}

	/**
	 * check now columns.
	 * @return true if success.
	 */
	protected boolean checkNowColumns() {
		nowColumns = cmd.getOptionValues('n');	// now value columns.
		if (nowColumns != null) {
			if (Util.hasDuplicateValuesIgnoreCase(nowColumns)) {
				usage(options);
				duplicateValueFound("use NOW() for the column");
				return false;
			}
			if (Util.hasDuplicateValuesIgnoreCase(nowColumns, columns)) {
				usage(options);
				duplicateValueFoundIn("use NOW() for the column", "column name to replace value");
				return false;
			}
		}
		return true;
	}

	/**
	 * check null columns.
	 * @return true if success.
	 */
	protected boolean checkNullColumns() {
		nulls = cmd.getOptionValues('N');	// null value columns.
		if (nulls != null) {
			if (Util.hasDuplicateValuesIgnoreCase(nulls)) {
				usage(options);
				duplicateValueFound("use NOW() for the column");
				return false;
			}
			if (Util.hasDuplicateValuesIgnoreCase(nulls, columns)) {
				usage(options);
				duplicateValueFoundIn("use null for the column", "column name to replace value");
				return false;
			}
			if (nowColumns != null) {
				if (Util.hasDuplicateValuesIgnoreCase(nulls, nowColumns)) {
					usage(options);
					duplicateValueFoundIn("use null for the column", "use column name to use default value");
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * get replace values short option.
	 * @return replace values short option.
	 */
	protected abstract char replaceValueShortOption();

	// MARK: - Usage
	/**
	 * show usage.
	 * @param options argument options.
	 */
	protected void usage(Options options) {
		usage(options, getClass().getSimpleName());
	}

	/**
	 * show usage.
	 * @param options argument options.
	 * @param appName application name.
	 */
	protected static void usage(Options options, String appName) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(appName, options);
	}

	/**
	 * show error message for prompt and password assigned at same time.
	 */
	private static void doNotAssignPromptAndPasswordAtSameTime() {
		System.out.println("[ERROR] do not assign prompt and password at same time.");
	}

	/**
	 * show error message options of too much values.
	 * @param argName argument name.
	 */
	private static void noMultipleOptions(String argName) {
		System.out.println("[ERROR] you have assigned too much values for " + argName);
	}

	/**
	 * show error message duplicate value has found.
	 * @param argName argument name.
	 */
	protected static void duplicateValueFound(String argName) {
		System.out.println("[ERROR] you have assigned duplicate values for " + argName);
	}

	/**
	 * show error message duplicate value has found in another argument.
	 * @param argName argument name.
	 * @param argName2 another argument name.
	 */
	protected static void duplicateValueFoundIn(String argName, String argName2) {
		System.out.println("[ERROR] you cannot assign " + argName + " which assigned in " + argName2);
	}

	/**
	 * show error message for columns and replaces counts are not same.
	 */
	protected abstract void columnAndReplaceCountMustSame();
}
