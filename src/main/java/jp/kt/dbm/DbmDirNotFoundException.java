package jp.kt.dbm;

import jp.kt.exception.KtException;

/**
 * DBMのディレクトリが存在しない場合のException.
 *
 * @author tatsuya.kumon
 */
public class DbmDirNotFoundException extends KtException {
	/** エラーコード */
	private static final String CODE = "A027";

	/**
	 * コンストラクタ.
	 *
	 * @param dir
	 *            ディレクトリ
	 */
	public DbmDirNotFoundException(String dir) {
		super(CODE, "DBMのディレクトリ " + dir + " が存在しません");
	}
}
