package jp.co.comona.javamisc.sql;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

/**
 * insert SQL record tool.
 * date: 2021/01/10
 * @author Toru Kageyama <info@comona.co.jp>
 */
public class InsertRecord extends SQLRecord {

	// MARK: - Static Properties
	private static final char REPLACE_SHORT_OPTION = 'v';
	private static final String REPLACE_SHORT_OPTION_STR = "" + REPLACE_SHORT_OPTION;

	// MARK: - Constructor
	/**
	 * constructor.
	 * @param options command line options.
	 * @param cmd command line.
	 */
	protected InsertRecord(Options options, CommandLine cmd) {
		super(options, cmd);
	}

	// MARK: - Process
	/**
	 * do process.
	 */
	@Override
	protected void doProcess() throws Exception {
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
				if (isNowValueColumn(columnIndex)) {
					sql.append(SQL_NOW);
				}
				else if (isNullValueColumn(columnIndex)) {
					sql.append(SQL_NULL);
				}
				else {
					sql.append(SQL_DEFAULT);
				}
				defaultCount++;
			}
		}
		sql.append(SQL_VALUES_END);
		//System.out.println(sql.toString());

		// create prepared statement.
		try (PreparedStatement ps = con.prepareStatement(sql.toString())) {
			defaultCount = 0;
			for (int i = 0; i < meta.getColumnCount(); i++) {
				int columnIndex = i + 1;
				String colName = meta.getColumnName(columnIndex);
				int columnType = meta.getColumnType(columnIndex);
				if (!putColumnValue(ps, columnIndexMap, colName, columnType)) {
					if (isNowValueColumn(columnIndex) || isNullValueColumn(columnIndex)) {
						defaultCount++;
					}
				}
			}
			ps.execute();
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
		else if (!cmd.hasOption('d') || !cmd.hasOption('t') || !cmd.hasOption('u')) {
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
		if (!checkColumnsAndReplaces()) {
			return -1;
		}
		if (!checkUserPassword()) {
			return -1;
		}
		if (!checkNowColumns()) {
			return -1;
		}
		if (!checkNullColumns()) {
			return -1;
		}

		return 0;
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
		options.addOption("c", "column-name", true, "column name to set value");
		options.addOption(REPLACE_SHORT_OPTION_STR, "column-value", true, "a value for column");
		options.addOption("n", "now", true, "use NOW() for the column");
		options.addOption("N", "null", true, "use null for the column");
		CommandLineParser parser = new DefaultParser();
		try {
			CommandLine cmd = parser.parse(options, args);
			InsertRecord insertRecord = new InsertRecord(options, cmd);
			int argCheck = insertRecord.checkArguments();
			if (argCheck != 0) {
				System.exit(argCheck < 0 ? ERROR_VALUE : SUCCESS_VALUE);
			}

			insertRecord.connectAndProcess();
		}
		catch (org.apache.commons.cli.ParseException e) {
			e.printStackTrace(System.err);
			usage(options, "InsertRecord");
			System.exit(ERROR_VALUE);
		}
		catch (Exception e1) {
			e1.printStackTrace(System.err);
			System.exit(ERROR_VALUE);
		}
	}

	/**
	 * show error message for columns and replaces counts are not same.
	 */
	@Override
	protected void columnAndReplaceCountMustSame() {
		System.out.println("[ERROR] column and value arguments count must be same.");
	}
}
