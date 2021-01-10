package jp.co.comona.javamisc.sql;

import jp.co.comona.javamisc.Util;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * copy SQL record tool.
 * date: 2021/01/09
 * @author Toru Kageyama <info@comona.co.jp>
 */
public class CopyRecord extends SQLRecord {

	// MARK: - Static Properties
	private static final char REPLACE_SHORT_OPTION = 'r';
	private static final String REPLACE_SHORT_OPTION_STR = "" + REPLACE_SHORT_OPTION;

	// MARK: - Properties
	private String[] keys = null;
	private String[] values = null;
	private String[] defaults = null;

	// MARK: - Constructor
	/**
	 * constructor.
	 * @param options command line options.
	 * @param cmd command line.
	 */
	protected CopyRecord(Options options, CommandLine cmd) {
		super(options, cmd);
	}

	// MARK: - Process
	/**
	 * do process.
	 */
	@Override
	protected void doProcess() throws Exception {
		StringBuilder sql = new StringBuilder(SQL_SELECT_FROM);
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

	/**
	 * duplicate record.
	 * @param rs result set.
	 * @throws Exception when error.
	 */
	private void duplicateRecord(ResultSet rs) throws Exception {
		StringBuilder sql = new StringBuilder(SQL_INSERT_INTO);
		sql.append(tableName).append(SQL_VALUES_START);
		Map<String, Integer> columnIndexMap = new HashMap<>();
		int defaultCount = 0;
		for (int i = 0; i < meta.getColumnCount(); i++) {
			int columnIndex = i + 1;
			if (i > 0) {
				sql.append(SQL_COMMA);
			}
			String colName = meta.getColumnName(columnIndex);
			if (putColumnIndex(columnIndexMap, colName, columnIndex - defaultCount)) {
				sql.append(SQL_PREPARED_MARK);
			}
			else {
				if (isDefaultValueColumn(columnIndex)) {
					sql.append(SQL_DEFAULT);
					defaultCount++;
				}
				else if (isNowValueColumn(columnIndex)) {
					sql.append(SQL_NOW);
					defaultCount++;
				}
				else if (isNullValueColumn(columnIndex)) {
					sql.append(SQL_NULL);
					defaultCount++;
				}
				else {
					sql.append(SQL_PREPARED_MARK);
				}
			}
		}
		sql.append(SQL_VALUES_END);

		// create prepared statement.
		try (PreparedStatement ps = con.prepareStatement(sql.toString())) {
			defaultCount = 0;
			for (int i = 0; i < meta.getColumnCount(); i++) {
				int columnIndex = i + 1;
				String colName = meta.getColumnName(columnIndex);
				int columnType = meta.getColumnType(columnIndex);
				if (!putColumnValue(ps, columnIndexMap, colName, columnType)) {
					if (isDefaultValueColumn(columnIndex) || isNowValueColumn(columnIndex) || isNullValueColumn(columnIndex)) {
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
			case Types.BLOB:
				ps.setBlob(psIndex, rs.getBlob(rsIndex));
				break;
			case Types.CLOB:
				ps.setClob(psIndex, rs.getClob(rsIndex));
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

	// MARK: - Check
	/**
	 * check arguments.
	 */
	@Override
	protected int checkArguments() {
		if (cmd.hasOption('?')) {
			usage(options);
			return 1;
		}
		else if (!cmd.hasOption('d') || !cmd.hasOption('t') || !cmd.hasOption('u') ||
				!cmd.hasOption('k') || !cmd.hasOption('v') || !cmd.hasOption('c') || !cmd.hasOption('r')) {
			usage(options);
			return -1;
		}
		if (!checkDatabaseName()) {
			return -1;
		}
		if (!checkTableName()) {
			return -1;
		}
		if (!checkHostName()) {
			return -1;
		}
		if (!checkUserName()) {
			return -1;
		}

		keys = cmd.getOptionValues('k');	// key & value count check.
		if (Util.hasDuplicateValuesIgnoreCase(keys)) {
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
		if (!checkColumnsAndReplaces()) {
			return -1;
		}
		if (!checkUserPassword()) {
			return -1;
		}
		defaults = cmd.getOptionValues('D');	// default value columns.
		if (defaults != null) {
			if (Util.hasDuplicateValuesIgnoreCase(defaults)) {
				usage(options);
				duplicateValueFound("column name to use default value");
				return -1;
			}
			if (Util.hasDuplicateValuesIgnoreCase(defaults, columns)) {
				usage(options);
				duplicateValueFoundIn("use column name to use default value", "column name to replace value");
				return -1;
			}
		}
		if (!checkNowColumns()) {
			return -1;
		}
		if (nowColumns != null) {
			if (defaults != null) {
				if (Util.hasDuplicateValuesIgnoreCase(nowColumns, defaults)) {
					usage(options);
					duplicateValueFoundIn("use NOW() for the column", "use column name to use default value");
					return -1;
				}
			}
		}
		if (!checkNullColumns()) {
			return -1;
		}
		if (nulls != null) {
			if (defaults != null) {
				if (Util.hasDuplicateValuesIgnoreCase(nulls, defaults)) {
					usage(options);
					duplicateValueFoundIn("use null for the column", "use NOW() for the column");
					return -1;
				}
			}
		}

		return 0;
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

	/**
	 * get replace values short option.
	 */
	@Override
	protected char replaceValueShortOption() {
		return REPLACE_SHORT_OPTION;
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
		options.addOption("p", "prompt", false, "[prompt password] do not set with -P");
		options.addOption("P", "password", true, "[user password] do not set with -p");
		options.addOption("k", "key-name", true, "key name");
		options.addOption("v", "key-value", true, "key value");
		options.addOption("c", "column-name", true, "column name to replace value");
		options.addOption(REPLACE_SHORT_OPTION_STR, "replace-value", true, "replace value for column");
		options.addOption("D", "default-value", true, "use default value for the column");
		options.addOption("n", "now", true, "use NOW() for the column");
		options.addOption("N", "null", true, "use null for the column");
		CommandLineParser parser = new DefaultParser();
		try {
			CommandLine cmd = parser.parse(options, args);
			CopyRecord copyRecord = new CopyRecord(options, cmd);
			int argCheck = copyRecord.checkArguments();
			if (argCheck != 0) {
				System.exit(argCheck < 0 ? ERROR_VALUE : SUCCESS_VALUE);
			}

			copyRecord.connectAndProcess();
		}
		catch (org.apache.commons.cli.ParseException e) {
			e.printStackTrace(System.err);
			usage(options, "CopyRecord");
			System.exit(ERROR_VALUE);
		}
		catch (Exception e1) {
			e1.printStackTrace(System.err);
			System.exit(ERROR_VALUE);
		}
	}

	// MARK: - Usage
	/**
	 * show error message for keys anv values counts are not same.
	 */
	private static void keyAndValueCountMustSame() {
		System.out.println("[ERROR] key and value arguments count must be same.");
	}

	/**
	 * show error message for columns and replaces counts are not same.
	 */
	@Override
	protected void columnAndReplaceCountMustSame() {
		System.out.println("[ERROR] column and replace arguments count must be same.");
	}
}
