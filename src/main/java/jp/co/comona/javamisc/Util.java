package jp.co.comona.javamisc;

/**
 * utility class.
 * date: 2021/01/10
 * @author Toru Kageyama <info@comona.co.jp>
 */
public class Util {

	// MARK: - Constructor
	/**
	 * constructor.
	 */
	private Util() {
		super();
	}

	// MARK: - Utility
	/**
	 * check for the duplication ignore case.
	 * @param keys string array to check.
	 * @return true when duplication found.
	 */
	public static boolean hasDuplicateValuesIgnoreCase(String[] keys) {
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
	 * check for the duplication ignore case.
	 * @param keys string array to check.
	 * @return true when duplication found.
	 */
	public static boolean hasDuplicateValuesIgnoreCase(String[] keys, String[] keys2) {
		for (String o1 : keys) {
			for (String o2 : keys2) {
				if (o1.compareToIgnoreCase(o2) == 0) {
					return true;
				}
			}
		}
		return false;
	}
}
