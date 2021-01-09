package jp.co.comona.javamisc;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.io.BufferedReader;
import java.io.Console;
import java.io.InputStreamReader;
import java.io.IOException;
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
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

/**
 * copy SQL record tool.
 * date: 2021/01/09
 * @author Toru Kageyama <info@comona.co.jp>
 */
public class CopyRecord {

	// MARK: - Static Properties
	private static final int SUCCESS_VALUE = 0;
	private static final int ERROR_VALUE = -1;

	// MARK: - Properties
	private final Options options;
	private final CommandLine cmd;
	private Connection con = null;
	private ResultSetMetaData meta = null;

	private String databaseName = null;
	private String tableName = null;
	private String hostName = "localhost";
	private String userName = null;
	private String password = null;
	private String[] keys = null;
	private String[] values = null;
	private String[] columns = null;
	private String[] replaces = null;
	private String[] defaults = null;
	private boolean prompt = false;

	// MARK: - Constructor
	/**
	 * constructor.
	 * @param options command line options.
	 * @param cmd command line.
	 */
	protected CopyRecord(Options options, CommandLine cmd) {
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
	private boolean connect() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");	// load MySQL database driver.

		String url =  "jdbc:mysql://" + hostName + ":3306/" + databaseName + "?useUnicode=true&amp;autoReconnect=true&amp;characterEncoding=utf8";
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
			return con != null;
		}
		else if (password != null) {
			con = DriverManager.getConnection(url, userName, password);
		}

		return true;
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
	private void disconnect() throws SQLException {
		if (con != null) {
			con.close();
		}
	}

	/**
	 * load metadata.
	 * @throws SQLException when SQL error.
	 */
	private void loadMetadata() throws SQLException {
		try (Statement stmt = con.createStatement(); ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " LIMIT 1 OFFSET 0")) {
			meta = rs.getMetaData();
		}
	}

	/**
	 * do process.
	 * @throws Exception when error.
	 */
	private void doProcess() throws Exception {
		StringBuilder sql = new StringBuilder("SELECT * FROM ");
		sql.append(tableName).append(" WHERE ");
		String key = keys[0];
		sql.append(key).append(" = ?");
		for (int i = 1; i < keys.length; i++) {
			key = keys[i];
			sql.append(' ').append(key).append(" = ?");
		}

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = con.prepareStatement(sql.toString());
			for (int i = 0; i < values.length; i++) {
				key = keys[i];
				String value = values[i];
				setPreparedStatement(ps, i + 1, key, value);
			}
			rs = ps.executeQuery();
			while (rs.next()) {
				duplicateRecord(rs);
			}
		}
		finally {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
		}
	}

	private void duplicateRecord(ResultSet rs) throws Exception {
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(tableName).append(" VALUES(");
		Map<String, Integer> columnIndexMap = new HashMap<>();
		int defaultCount = 0;
		for (int i = 0; i < meta.getColumnCount(); i++) {
			int columnIndex = i + 1;
			if (i > 0) {
				sql.append(", ");
			}
			String colName = meta.getColumnName(columnIndex);
			if (putColumnIndex(columnIndexMap, colName, columnIndex - defaultCount)) {
				sql.append('?');
			}
			else {
				if (isDefaultValueColumn(columnIndex)) {
					sql.append("DEFAULT");
					defaultCount++;
				}
				else {
					sql.append('?');
				}
			}
		}

		sql.append(')');

		// create prepared statement.
		try (PreparedStatement ps = con.prepareStatement(sql.toString())) {
			defaultCount = 0;
			for (int i = 0; i < meta.getColumnCount(); i++) {
				int columnIndex = i + 1;
				String colName = meta.getColumnName(columnIndex);
				int columnType = meta.getColumnType(columnIndex);
				if (!putColumnValue(ps, columnIndexMap, colName, columnType)) {
					if (isDefaultValueColumn(columnIndex)) {
						defaultCount++;
					} else {
						copyColumn(rs, columnIndex, ps, columnIndex - defaultCount, columnType);
					}
				}
			}
			ps.execute();
		}
	}

	/**
	 * put prepare statement index to column.
	 * @param columnIndexMap column to prepared statement index map.
	 * @param colName column name.
	 * @param columnIndex column prepared statement index.
	 * @return true if index put to the map.
	 */
	private boolean putColumnIndex(Map<String, Integer> columnIndexMap, String colName, int columnIndex) {
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
	private boolean putColumnValue(PreparedStatement ps, Map<String, Integer> columnIndexMap, String colName, int columnType) throws Exception {
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

	/**
	 * copy column value from result set to prepared statement.
	 * @param rs result set.
	 * @param rsIndex column index of result set.
	 * @param ps prepared statement.
	 * @param psIndex column index if prepared statement.
	 * @param columnType column type.
	 * @throws SQLException when SQL error.
	 */
	private void copyColumn(ResultSet rs, int rsIndex, PreparedStatement ps, int psIndex, int columnType) throws SQLException {
		switch (columnType) {
			case Types.INTEGER:
				ps.setInt(psIndex, rs.getInt(rsIndex));
			case Types.BIGINT:
			case Types.DECIMAL:
				ps.setLong(psIndex, rs.getLong(rsIndex));
				break;
			case Types.SMALLINT:
			case Types.TINYINT:
				ps.setShort(psIndex, rs.getShort(rsIndex));
				break;
			case Types.FLOAT:
				ps.setFloat(psIndex, rs.getFloat(rsIndex));
				break;
			case Types.DOUBLE:
			case Types.NUMERIC:
				ps.setDouble(psIndex, rs.getDouble(rsIndex));
				break;
			case Types.DATE:
				ps.setDate(psIndex, rs.getDate(rsIndex));
				break;
			case Types.TIMESTAMP:
				ps.setTimestamp(psIndex, rs.getTimestamp(rsIndex));
				break;
			case Types.TIME:
				ps.setTime(psIndex, rs.getTime(rsIndex));
				break;

			default:
				ps.setString(psIndex, rs.getString(rsIndex));
				break;
		}
	}

	/**
	 * set value to prepared statement.
	 * @param ps prepared statement.
	 * @param index index of prepared statement.
	 * @param key key name.
	 * @param value key value.
	 * @throws Exception when error.
	 */
	void setPreparedStatement(PreparedStatement ps, int index, String key, String value) throws Exception {
		for (int i = 0; i < meta.getColumnCount(); i++) {
			int columnIndex = i + 1;
			if (key.compareToIgnoreCase(meta.getColumnName(columnIndex)) == 0) {
				int type = meta.getColumnType(columnIndex);
				switch (type) {
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
							throw new Exception("TINYINT out of range.");
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
				break;
			}
		}
	}

	/**
	 * convert string to date.
	 * @param value date value in string.
	 * @return date.
	 * @throws java.text.ParseException when parser error.
	 */
	private Date convertDate(String value) throws ParseException {
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
	private Date convertDateTime(String value) throws ParseException {
		SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		return new Date(sdFormat.parse(value).getTime());
	}

	/**
	 * convert string to date.
	 * @param value date value in string.
	 * @return date.
	 * @throws java.text.ParseException when parser error.
	 */
	private Time convertTime(String value) throws ParseException {
		SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		return new Time(sdFormat.parse("1970-01-01 " + value).getTime());
	}

	/**
	 * convert string to date.
	 * @param value date value in string.
	 * @return date.
	 * @throws java.text.ParseException when parser error.
	 */
	private Timestamp convertTimestamp(String value) throws ParseException {
		SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		return new Timestamp(sdFormat.parse(value).getTime());
	}

	// MARK: - Check
	/**
	 * check arguments.
	 * @return 0 if success, positive if help, negative if error.
	 */
	private int checkArguments() {
		if (cmd.hasOption('?')) {
			usage(options);
			return 1;
		}
		else if (!cmd.hasOption('d') || !cmd.hasOption('t') || !cmd.hasOption('u') ||
				!cmd.hasOption('k') || !cmd.hasOption('v') || !cmd.hasOption('c') || !cmd.hasOption('r')) {
			usage(options);
			return -1;
		}
		String[] databases = cmd.getOptionValues('d');	// database name check.
		if (databases.length > 1) {
			usage(options);
			noMultipleOptions("database name");
			return -1;
		}
		databaseName = databases[0];
		String[] tables = cmd.getOptionValues('t');	// table name check.
		if (tables.length > 1) {
			usage(options);
			noMultipleOptions("table name");
			return -1;
		}
		tableName = tables[0];
		String[] hosts = cmd.getOptionValues('h');	// host name check.
		if (hosts != null) {
			if (hosts.length > 1) {
				usage(options);
				noMultipleOptions("host name");
				return -1;
			}
			hostName = hosts[0];
		}
		String[] users = cmd.getOptionValues('u');	// user name check.
		if (users.length > 1) {
			usage(options);
			noMultipleOptions("user name");
			return -1;
		}
		userName = users[0];

		keys = cmd.getOptionValues('k');	// key & value count check.
		if (hasDuplicateValues(keys)) {
			usage(options);
			duplicateValueFound("key name");
			return -1;
		}
		values = cmd.getOptionValues('v');
		if (keys.length != values.length) {
			usage(options);
			keyAndValueCountMustSame();
			return -1;
		}
		columns = cmd.getOptionValues('c');	// column & replace count check.
		if (hasDuplicateValues(columns)) {
			usage(options);
			duplicateValueFound("column name to replace value");
			return -1;
		}
		replaces = cmd.getOptionValues('r');
		if (columns.length != replaces.length) {
			usage(options);
			columnAndReplaceCountMustSame();
			return -1;
		}
		prompt = cmd.hasOption('p');
		if (prompt && cmd.hasOption('P')) {	// password & prompt option check.
			usage(options);
			doNotAssignPromptAndPasswordAtSameTime();
			return -1;
		}
		String[] passwords = cmd.getOptionValues('P');	// password check.
		if (passwords != null) {
			if (passwords.length > 1) {
				usage(options);
				noMultipleOptions("user password");
				return -1;
			}
			password = passwords[0];
		}
		defaults = cmd.getOptionValues('D');	// default value columns.
		if (defaults != null) {
			if (hasDuplicateValues(defaults)) {
				usage(options);
				duplicateValueFound("column name to use default value");
				return -1;
			}
			if (hasDuplicateValues(defaults, columns)) {
				usage(options);
				duplicateValueFoundIn();
				return -1;
			}
		}

		return 0;
	}

	/**
	 * check for the duplication.
	 * @param keys string array to check.
	 * @return true when duplication found.
	 */
	private static boolean hasDuplicateValues(String[] keys) {
		for (int i = 0; i < keys.length - 1; i++) {
			String o1 = keys[i];
			for (int j = i + 1; j < keys.length; j++) {
				String o2 = keys[j];
				if (o1.compareToIgnoreCase(o2) == 0) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * check for the duplication.
	 * @param keys string array to check.
	 * @return true when duplication found.
	 */
	private static boolean hasDuplicateValues(String[] keys, String[] keys2) {
		for (String o1 : keys) {
			for (String o2 : keys2) {
				if (o1.compareToIgnoreCase(o2) == 0) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * is default value column.
	 * @param columnIndex column index.
	 * @return true if use default value.
	 * @throws SQLException when SQL error.
	 */
	private boolean isDefaultValueColumn(int columnIndex) throws SQLException {
		if (meta.isAutoIncrement(columnIndex)) {
			return true;
		}
		if (defaults != null) {
			for (String defaultColumn : defaults) {
				if (defaultColumn.compareToIgnoreCase(meta.getColumnName(columnIndex)) == 0) {
					return true;
				}
			}
		}
		return false;
	}

	// MARK: - Entry Point
	/**
	 * entry point.
	 * @param args arguments array.
	 */
	public static void main(String[] args) {
		Options options = new Options();
		options.addOption("?", "help", false, "show this help");
		options.addOption("d", "database", true, "database name");
		options.addOption("t", "table", true, "table name");
		options.addOption("h", "host", true, "[host name, localhost as default]");
		options.addOption("u", "user", true, "user name");
		options.addOption("p", "prompt", false, "[prompt password]");
		options.addOption("P", "password", true, "[user password]");
		options.addOption("k", "key-name", true, "key name");
		options.addOption("v", "key-value", true, "key value");
		options.addOption("c", "column-name", true, "column name to replace value");
		options.addOption("r", "replace-value", true, "replace value for column");
		options.addOption("D", "default-value", true, "use default value for the column");
		CommandLineParser parser = new DefaultParser();
		try {
			CommandLine cmd = parser.parse(options, args);
			CopyRecord copyRecord = new CopyRecord(options, cmd);
			int argCheck = copyRecord.checkArguments();
			if (argCheck != 0) {
				System.exit(argCheck < 0 ? ERROR_VALUE : SUCCESS_VALUE);
			}

			if (copyRecord.connect()) {
				copyRecord.loadMetadata();
				copyRecord.doProcess();
				copyRecord.disconnect();
			}
		}
		catch (org.apache.commons.cli.ParseException e) {
			e.printStackTrace(System.err);
			usage(options);
			System.exit(ERROR_VALUE);
		}
		catch (Exception e1) {
			e1.printStackTrace(System.err);
			System.exit(ERROR_VALUE);
		}
	}

	// MARK: - Usage
	/**
	 * show usage.
	 * @param options argument options.
	 */
	private static void usage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("CopyRecord", options);
	}

	/**
	 * show error message for keys anv values counts are not same.
	 */
	private static void keyAndValueCountMustSame() {
		System.out.println("[ERROR] key and value arguments count must be same.");
	}

	/**
	 * show error message for columns and replaces counts are not same.
	 */
	private static void columnAndReplaceCountMustSame() {
		System.out.println("[ERROR] column and replace arguments count must be same.");
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
	private static void duplicateValueFound(String argName) {
		System.out.println("[ERROR] you have assigned duplicate values for " + argName);
	}

	/**
	 * show error message duplicate value has found in another argument.
	 */
	private static void duplicateValueFoundIn() {
		System.out.println("[ERROR] you cannot use column name to use default value which assigned in column name to replace value");
	}
}
