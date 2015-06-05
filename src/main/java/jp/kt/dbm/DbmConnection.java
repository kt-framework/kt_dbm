package jp.kt.dbm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * DBM接続情報を管理するクラス.
 *
 * @author tatsuya.kumon
 */
public class DbmConnection implements Serializable {
	private static final long serialVersionUID = 1L;

	/** 保持するDBMリスト */
	private List<Dbm> dbmList;

	/** 最大ファイルロック秒数（デフォルト10秒） */
	private int fileLockSec = 10;

	/** DBMファイルのパーミッション */
	private String filePermission;

	/**
	 * DBMファイルの最大ロック秒数を指定する.
	 * <p>
	 * デフォルトは10秒.<br>
	 * loadメソッドよりも前に実行すること.
	 * </p>
	 *
	 * @param fileLockSec
	 *            最大ファイルロック秒数
	 */
	public void setFileLockSec(int fileLockSec) {
		this.fileLockSec = fileLockSec;
	}

	/**
	 * DBMファイルのパーミッションを指定する.
	 * <p>
	 * このメソッドを実行しなかった場合は、サーバで設定されたデフォルトのパーミッション設定となる.<br>
	 * loadメソッドよりも前に実行すること.
	 * </p>
	 *
	 * @param filePermission
	 *            ファイルパーミッション.<br>
	 *            3桁数字で指定する
	 */
	public void setFilePermission(String filePermission) {
		this.filePermission = filePermission;
	}

	/**
	 * DBMをロードする.
	 * <p>
	 * 最大ファイルロック待ち秒数はデフォルト値となります.
	 * </p>
	 *
	 * @param dir
	 *            DBMファイルが存在するディレクトリ
	 * @param dbmName
	 *            DBM名（ファイル名ではない）
	 * @param mode
	 *            {@link DbmMode} クラスの定数で指定.
	 * @return ロードされた {@link Dbm} オブジェクト
	 * @throws Exception
	 *             DBMファイルロード時に例外発生した場合
	 */
	public Dbm load(String dir, String dbmName, DbmMode mode) throws Exception {
		// DBMのロード
		Dbm dbm = new Dbm(dir, dbmName, mode, fileLockSec, filePermission);
		// DBMリストに追加
		addDbm(dbm);
		return dbm;
	}

	/**
	 * DBMをロードする.
	 * <p>
	 * 最大ファイルロック待ち秒数はデフォルト値となります.
	 * </p>
	 *
	 * @param dbmFilePath
	 *            DBMファイルのパス
	 * @param mode
	 *            {@link DbmMode} クラスの定数で指定.
	 * @return ロードされた {@link Dbm} オブジェクト
	 * @throws Exception
	 *             DBMファイルロード時に例外発生した場合
	 */
	public Dbm load(String dbmFilePath, DbmMode mode) throws Exception {
		// DBMのロード
		Dbm dbm = new Dbm(dbmFilePath, mode, fileLockSec, filePermission);
		// DBMリストに追加
		addDbm(dbm);
		return dbm;
	}

	/**
	 * DBMをリストに追加.
	 *
	 * @param dbm
	 *            DBM
	 */
	private void addDbm(Dbm dbm) {
		if (dbmList == null) {
			dbmList = new ArrayList<Dbm>();
		}
		dbmList.add(dbm);
	}

	/**
	 * このDBM接続が保持しているDBMリストを返す.
	 * <p>
	 * 既にcloseされているDBMオブジェクトは含まれません.
	 * </p>
	 *
	 * @return 保持しているDBM
	 */
	public List<Dbm> getActiveDbmList() {
		List<Dbm> list = new ArrayList<Dbm>();
		if (dbmList != null) {
			for (Dbm dbm : dbmList) {
				if (!dbm.isCompleteWrite()) {
					list.add(dbm);
				}
			}
		}
		return list;
	}
}
