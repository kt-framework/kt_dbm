package jp.kt.dbm;

import java.io.Serializable;

/**
 * 操作モード.
 *
 * @author tatsuya.kumon
 */
public class DbmMode implements Serializable {
	private static final long serialVersionUID = 1L;

	private int mode;

	private String text;

	/**
	 * 読み取り専用モード.
	 * <p>
	 * このモードの場合はロックはかかりません.
	 * </p>
	 */
	public static final DbmMode READ_ONLY = new DbmMode(1, "読み取り専用モード");

	/**
	 * データ読み書きモード.
	 * <p>
	 * DBMファイル作成や削除は不可.
	 * </p>
	 */
	public static final DbmMode READ_AND_WRITE = new DbmMode(2, "データ読み書きモード");

	/** テーブル作成や削除が可能.データの書き込みも可. */
	public static final DbmMode ALL_OPERATE = new DbmMode(3, "全操作可能モード");

	/**
	 * 内部コンストラクタ.
	 *
	 * @param mode
	 *            操作モード
	 * @param text
	 *            モード文言
	 */
	private DbmMode(int mode, String text) {
		this.mode = mode;
		this.text = text;
	}

	/**
	 * モード文言を取得する.
	 *
	 * @return モード文言
	 */
	public String getText() {
		return text;
	}

	/*
	 * (非 Javadoc)
	 *
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof DbmMode) {
			if (((DbmMode) obj).mode == this.mode) {
				return true;
			}
		}
		return false;
	}
}
