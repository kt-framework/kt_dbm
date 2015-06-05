package jp.kt.dbm;

import jp.kt.exception.KtException;
import jp.kt.tool.Validator;

/**
 * 操作エラー.
 *
 * @author tatsuya.kumon
 */
public class DbmOperationException extends KtException {
	private static final long serialVersionUID = 1L;

	/** エラーコード */
	private static final String CODE = "A029";

	/**
	 * コンストラクタ.
	 *
	 * @param message
	 *            メッセージ
	 * @param dbmFilePath
	 *            DBMファイルパス
	 */
	public DbmOperationException(String message, String dbmFilePath) {
		super(CODE, createMessage(message, dbmFilePath, null, null));
	}

	/**
	 * コンストラクタ.
	 *
	 * @param message
	 *            メッセージ
	 * @param dbmFilePath
	 *            DBMファイルパス
	 * @param key
	 *            キー
	 */
	public DbmOperationException(String message, String dbmFilePath, String key) {
		super(CODE, createMessage(message, dbmFilePath, key, null));
	}

	/**
	 * コンストラクタ.
	 *
	 * @param message
	 *            メッセージ
	 * @param dbmFilePath
	 *            DBMファイルパス
	 * @param key
	 *            キー
	 * @param data
	 *            値
	 */
	public DbmOperationException(String message, String dbmFilePath,
			String key, String data) {
		super(CODE, createMessage(message, dbmFilePath, key, data));
	}

	/**
	 * エラーメッセージの生成.
	 *
	 * @param message
	 *            メッセージ
	 * @param dbmFilePath
	 *            DBMファイルパス
	 * @param key
	 *            キー
	 * @param data
	 *            値
	 * @return エラーメッセージ
	 */
	private static String createMessage(String message, String dbmFilePath,
			String key, String data) {
		StringBuilder msg = new StringBuilder();
		msg.append(message);
		msg.append(":");
		if (!Validator.isEmpty(dbmFilePath)) {
			msg.append(" [dbmFilePath=");
			msg.append(dbmFilePath);
			msg.append("]");
		}
		if (!Validator.isEmpty(key)) {
			msg.append(" [key=");
			msg.append(key);
			msg.append("]");
		}
		if (!Validator.isEmpty(data)) {
			msg.append(" [data=");
			msg.append(data);
			msg.append("]");
		}
		return msg.toString();
	}
}
