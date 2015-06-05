package jp.kt.dbm;

import jp.kt.exception.KtException;

/**
 * DBMのファイルが存在しない場合のException.
 *
 * @author tatsuya.kumon
 */
public class DbmFileNotFoundException extends KtException {
	private static final long serialVersionUID = 1L;

	/** エラーコード */
	private static final String CODE = "A028";

	/**
	 * コンストラクタ.
	 *
	 * @param dbmFilePath
	 *            DBMファイルパス
	 */
	public DbmFileNotFoundException(String dbmFilePath) {
		super(CODE, "DBMのファイル " + dbmFilePath + " が存在しません");
	}
}
