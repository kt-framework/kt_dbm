package jp.kt.dbm;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import jp.kt.fileio.FileLock;
import jp.kt.fileio.FileUtil;
import jp.kt.fileio.Find;
import jp.kt.fileio.FindCondition;
import jp.kt.fileio.FindCondition.Type;
import jp.kt.tool.Validator;

/**
 * DBM各種処理クラス.
 * <p>
 * コミットやロールバックは、Dbmオブジェクト単位で行うものであり、<br>
 * 1つのDbmオブジェクトに対して1回のみ実行可能です.<br>
 * よって、コミットやロールバック後は、書込み処理は不可ですが、読み込み処理は可能です.
 * </p>
 *
 * @author tatsuya.kumon
 */
public final class Dbm implements Serializable {
	/** 最大ロード回数 */
	private static final int MAX_LOAD_TIMES = 5;

	/** テーブルファイルの拡張子 */
	private static final String TABLE_FILE_EXT = ".ktdb";

	/** DBMファイルパス */
	private String dbmFilePath;

	/** 処理モード */
	private DbmMode mode;

	/** 最大ファイルロック秒数 */
	private int fileLockSec;

	/** DBMファイルのパーミッション */
	private String filePermission;

	/** メモリ上にロードするMap（rollback用） */
	private Map<String, String> orgMap;

	/** メモリ上にロードするMap（データ更新用） */
	private Map<String, String> recordMap;

	/** DBMファイルのロック */
	private FileLock lock;

	/** 書込み完了フラグ */
	private boolean isCompleteWrite;

	/**
	 * コンストラクタ.
	 * <p>
	 * DBMファイルが無ければ、<br>
	 * {@link DbmMode#ALL_OPERATE} モードの場合は、新規作成します.<br>
	 * それ以外のモードの場合は、{@link DbmFileNotFoundException} がthrowされます.
	 * </p>
	 *
	 * @param dir
	 *            ディレクトリ
	 * @param dbmName
	 *            DBM名
	 * @param mode
	 *            処理モード
	 * @param fileLockSec
	 *            最大ファイルロック秒数
	 * @param filePermission
	 *            ファイルパーミッション.<br>
	 *            3桁数字で指定する
	 * @throws Exception
	 */
	Dbm(String dir, String dbmName, DbmMode mode, int fileLockSec,
			String filePermission) throws Exception {
		FileUtil fileUtil = new FileUtil(dir);
		if (!fileUtil.isDirectory()) {
			// ディレクトリが存在しない場合はエラー
			throw new DbmDirNotFoundException(fileUtil.getPath());
		}
		fileUtil.setNextPath(dbmName + TABLE_FILE_EXT);
		// 初期処理
		init(fileUtil.getPath(), mode, fileLockSec, filePermission);
	}

	/**
	 * コンストラクタ.
	 * <p>
	 * DBMファイルが無ければ、<br>
	 * {@link DbmMode#ALL_OPERATE} モードの場合は、新規作成します.<br>
	 * それ以外のモードの場合は、{@link DbmFileNotFoundException} がthrowされます.
	 * </p>
	 *
	 * @param dbmFilePath
	 *            DBMファイルパス
	 * @param mode
	 *            処理モード
	 * @param fileLockSec
	 *            最大ファイルロック秒数
	 * @param filePermission
	 *            ファイルパーミッション.<br>
	 *            3桁数字で指定する
	 * @throws Exception
	 */
	Dbm(String dbmFilePath, DbmMode mode, int fileLockSec, String filePermission)
			throws Exception {
		// 初期処理
		init(dbmFilePath, mode, fileLockSec, filePermission);
	}

	/**
	 * コンストラクタ共通の初期処理.
	 *
	 * @param dbmFilePath
	 *            DBMファイルパス
	 * @param mode
	 *            処理モード
	 * @param fileLockSec
	 *            最大ファイルロック秒数
	 * @param filePermission
	 *            ファイルパーミッション.<br>
	 *            3桁数字で指定する
	 * @throws Exception
	 */
	private void init(String dbmFilePath, DbmMode mode, int fileLockSec,
			String filePermission) throws Exception {
		this.dbmFilePath = dbmFilePath;
		this.mode = mode;
		this.fileLockSec = fileLockSec;
		this.filePermission = filePermission;
		this.isCompleteWrite = false;
		// DBMファイルのロード
		load();
	}

	/**
	 * DBMファイルのロード.
	 * <p>
	 * DBMファイルが無ければ、<br>
	 * {@link DbmMode#ALL_OPERATE} モードの場合は、新規作成します.<br>
	 * それ以外のモードの場合は、{@link DbmFileNotFoundException} がthrowされます.
	 * </p>
	 *
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private void load() throws Exception {
		FileUtil f = new FileUtil(dbmFilePath);
		boolean existFile = f.isFile();
		if (!existFile) {
			/*
			 * テーブルファイルが存在しない場合の処理
			 */
			if (mode.equals(DbmMode.ALL_OPERATE)) {
				// テーブル操作モードの場合はMap生成
				this.recordMap = new HashMap<String, String>();
			} else {
				// テーブル操作モードでない場合はException
				throw new DbmFileNotFoundException(dbmFilePath);
			}
		} else {
			/*
			 * テーブルファイルが存在する場合は読み込み
			 */
			lock = new FileLock(dbmFilePath, fileLockSec);
			if (mode.equals(DbmMode.READ_ONLY)) {
				// 読み取り専用モードの場合はロック解除されるのを待つ
				lock.waitRelease();
			} else {
				// 読み取り専用モード以外はファイルロックする
				lock.lock();
			}
			// 読み込み実行（最大5回までリトライする）
			for (int i = 1; i <= MAX_LOAD_TIMES; i++) {
				Object obj = null;
				ObjectInputStream ois = null;
				try {
					FileInputStream fis = new FileInputStream(dbmFilePath);
					BufferedInputStream bis = new BufferedInputStream(fis);
					ois = new ObjectInputStream(bis);
					obj = ois.readObject();
					// インスタンス変数にセット
					this.recordMap = (HashMap<String, String>) obj;
					// Exceptionが発生しなかったのでbreak;
					break;
				} catch (Exception e) {
					// Exceptionが発生したらリトライ
					if (i < MAX_LOAD_TIMES) {
						// リトライする前に0.5秒sleepする
						Thread.sleep(500);
					} else {
						// 最大リトライ回数に達したらExceptionをthrow
						throw e;
					}
				} finally {
					if (ois != null) {
						ois.close();
					}
				}
			}
		}
		// rollback用のMapにコピー
		this.orgMap = new HashMap<String, String>(this.recordMap);
	}

	/**
	 * rollback処理.
	 * <p>
	 * メモリ上のデータを元に戻す.
	 * </p>
	 */
	public void rollback() {
		// 読み取り専用モードの場合は何もしない
		if (mode.equals(DbmMode.READ_ONLY)) {
			return;
		}
		// このDBMが書込み完了いるかチェック
		if (isCompleteWrite()) {
			throw new DbmOperationException(
					"既にこのDBMは書込み完了済み(commitもしくはrollback済み)です", dbmFilePath);
		}
		// rollback
		if (this.orgMap != null) {
			this.recordMap = new HashMap<String, String>(this.orgMap);
		}
		// ファイルロック解除
		if (lock != null) {
			lock.release();
		}
		// 書込み完了処理
		completeWrite();
	}

	/**
	 * commit処理.
	 * <p>
	 * MapデータをDBMファイルに保存する.
	 * </p>
	 *
	 * @throws IOException
	 *             入出力エラーが発生した場合
	 */
	public void commit() throws IOException {
		// 読み取り専用モードの場合は何もしない
		if (mode.equals(DbmMode.READ_ONLY)) {
			return;
		}
		// このDBMが書込み完了いるかチェック
		if (isCompleteWrite()) {
			throw new DbmOperationException(
					"既にこのDBMは書込み完了済み(commitもしくはrollback済み)です", dbmFilePath);
		}
		// commit
		if (this.recordMap != null) {
			// DBMファイル存在確認
			FileUtil f = new FileUtil(this.dbmFilePath);
			boolean isNewFile = !f.isFile();
			// 書き込み処理
			if (isNewFile || !this.recordMap.equals(this.orgMap)) {
				// 新規ファイルもしくは内容が変更されている場合はDBMファイル出力
				ObjectOutputStream oos = null;
				try {
					FileOutputStream fos = new FileOutputStream(
							this.dbmFilePath);
					oos = new ObjectOutputStream(fos);
					oos.writeObject(this.recordMap);
					oos.reset();
				} finally {
					if (oos != null) {
						oos.close();
					}
				}
			}
			// パーミッション指定されていて、且つDBMファイル新規作成の場合はパーミッションを変更する
			if (!Validator.isEmpty(filePermission) && isNewFile) {
				f.chmod(filePermission);
			}
		}
		// ファイルロック解除
		if (lock != null) {
			lock.release();
		}
		// 書込み完了処理
		completeWrite();
	}

	/**
	 * キーに対する値を読み込む.
	 *
	 * @param key
	 *            キー
	 * @return 値
	 */
	public String read(String key) {
		return recordMap.get(key);
	}

	/**
	 * 全件読み込む.
	 * <p>
	 * キーの昇順でソートされたMapを返します.
	 * </p>
	 *
	 * @return レコード全件のMap
	 */
	public Map<String, String> readAll() {
		return new TreeMap<String, String>(recordMap);
	}

	/**
	 * 指定したキーより小さいレコードを読み込む.
	 * <p>
	 * キーの昇順でソートされたMapを返します.
	 * </p>
	 *
	 * @param toKey
	 *            境界となるキー値（取得するMapにこの値は含みません）
	 * @return 指定したキーより小さいレコードのMap
	 */
	public Map<String, String> readHead(String toKey) {
		return new TreeMap<String, String>(recordMap).headMap(toKey);
	}

	/**
	 * 指定したキーより大きいレコードを読み込む.
	 * <p>
	 * キーの昇順でソートされたMapを返します.
	 * </p>
	 *
	 * @param fromKey
	 *            境界となるキー値（取得するMapにこの値は含みません）
	 * @return 指定したキーより大きいレコードのMap
	 */
	public Map<String, String> readTail(String fromKey) {
		return new TreeMap<String, String>(recordMap).tailMap(fromKey);
	}

	/**
	 * レコードを1件書き込む.
	 * <p>
	 * データが存在しなければINSERT、存在すればUPDATEとなります.<br>
	 * {@link Dbm#commit()} が実行されるまでファイルには反映されません.<br>
	 * {@link DbmMode#READ_AND_WRITE} モード、もしくは{@link DbmMode#ALL_OPERATE}
	 * モードの場合のみ操作可能です.
	 * </p>
	 *
	 * @param key
	 *            キー
	 * @param data
	 *            値
	 */
	public void write(String key, String data) {
		// モードチェック
		if (mode.equals(DbmMode.READ_ONLY)) {
			throw new DbmOperationException(mode.getText()
					+ "にもかかわらず書き込もうとしました", dbmFilePath, key, data);
		}
		// このDBMが書込み完了いるかチェック
		if (isCompleteWrite()) {
			throw new DbmOperationException(
					"既にこのDBMは書込み完了済み(commitもしくはrollback済み)です", dbmFilePath);
		}
		// Mapにセット
		recordMap.put(key, data);
	}

	/**
	 * 複数レコードをまとめて書き込む.
	 * <p>
	 * データが存在しなければINSERT、存在すればUPDATEとなります.<br>
	 * {@link Dbm#commit()} が実行されるまでファイルには反映されません.<br>
	 * {@link DbmMode#READ_AND_WRITE} モード、もしくは{@link DbmMode#ALL_OPERATE}
	 * モードの場合のみ操作可能です.
	 * </p>
	 *
	 * @param recordMap
	 *            セットするMap
	 */
	public void write(Map<String, String> recordMap) {
		// モードチェック
		if (mode.equals(DbmMode.READ_ONLY)) {
			throw new DbmOperationException(mode.getText()
					+ "にもかかわらず複数レコード書き込もうとしました", dbmFilePath);
		}
		// このDBMが書込み完了いるかチェック
		if (isCompleteWrite()) {
			throw new DbmOperationException(
					"既にこのDBMは書込み完了済み(commitもしくはrollback済み)です", dbmFilePath);
		}
		// Mapにセット
		this.recordMap.putAll(recordMap);
	}

	/**
	 * 指定したキーのレコードを削除.
	 * <p>
	 * {@link Dbm#commit()} が実行されるまでファイルには反映されません.<br>
	 * {@link DbmMode#READ_AND_WRITE} モード、もしくは{@link DbmMode#ALL_OPERATE}
	 * モードの場合のみ操作可能です.
	 * </p>
	 *
	 * @param key
	 *            キー
	 */
	public void delete(String key) {
		// モードチェック
		if (mode.equals(DbmMode.READ_ONLY)) {
			throw new DbmOperationException(mode.getText()
					+ "にもかかわらずレコード削除しようとしました", dbmFilePath, key);
		}
		// このDBMが書込み完了いるかチェック
		if (isCompleteWrite()) {
			throw new DbmOperationException(
					"既にこのDBMは書込み完了済み(commitもしくはrollback済み)です", dbmFilePath);
		}
		// Mapから削除
		recordMap.remove(key);
	}

	/**
	 * 全レコードを削除.
	 * <p>
	 * {@link Dbm#commit()} が実行されるまでファイルには反映されません.<br>
	 * {@link DbmMode#READ_AND_WRITE} モード、もしくは{@link DbmMode#ALL_OPERATE}
	 * モードの場合のみ操作可能です.
	 * </p>
	 */
	public void deleteAll() {
		// モードチェック
		if (mode.equals(DbmMode.READ_ONLY)) {
			throw new DbmOperationException(mode.getText()
					+ "にもかかわらず全レコード削除しようとしました", dbmFilePath);
		}
		// このDBMが書込み完了いるかチェック
		if (isCompleteWrite()) {
			throw new DbmOperationException(
					"既にこのDBMは書込み完了済み(commitもしくはrollback済み)です", dbmFilePath);
		}
		// 全レコード削除（＝新しいインスタンスにする）
		this.recordMap = new HashMap<String, String>();
	}

	/**
	 * DBMファイルの削除.
	 * <p>
	 * 実行すると即時ファイルが削除され、復旧はできなくなります.<br>
	 * この操作を行った後でreadやwriteなどを実行すると Exception が発生します.<br>
	 * {@link DbmMode#ALL_OPERATE} モードの場合のみ操作可能です.<br>
	 * DBMファイルが存在しなくてもExceptionは発生しません.
	 * </p>
	 *
	 * @throws IOException
	 *             入出力エラーが発生した場合
	 */
	public void dropDbm() throws IOException {
		// モードチェック
		if (!mode.equals(DbmMode.ALL_OPERATE)) {
			throw new DbmOperationException(mode.getText()
					+ "にもかかわらずDBMファイル削除しようとしました", dbmFilePath);
		}
		// DBMファイル削除
		new FileUtil(dbmFilePath).delete();
		// 復活ができないようにMapを空にする
		this.orgMap = null;
		this.recordMap = null;
	}

	/**
	 * 書込み完了処理.
	 * <p>
	 * commitやrollbackが実行された場合に呼び出される.
	 * </p>
	 */
	private void completeWrite() {
		// 書込み完了フラグをONにする
		this.isCompleteWrite = true;
	}

	/**
	 * このDBMは書込み完了しているか判定.
	 * <p>
	 * 1つのDBMに対してcommitもしくはrollbackが実行された場合は書込み完了状態となる.
	 * </p>
	 *
	 * @return 書込み完了されている場合はtrue
	 */
	boolean isCompleteWrite() {
		return this.isCompleteWrite;
	}

	/**
	 * DBMファイルを再帰的に検索し、ファイルパスのリストを返します.
	 * <p>
	 * 対象ディレクトリ内を再帰的に検索します.
	 * </p>
	 *
	 * @param dir
	 *            対象ディレクトリ
	 * @return 検索されたDBMファイルパスのリスト
	 * @throws IOException
	 *             入出力エラーが発生した場合
	 */
	public static List<String> getDbmFileList(String dir) throws IOException {
		FileUtil d = new FileUtil(dir);
		if (!d.isDirectory()) {
			throw new DbmDirNotFoundException(dir);
		}
		// DBMファイルを拡張子でfind検索
		FindCondition condition = new FindCondition(dir, Type.ONLY_FILE, ".*\\"
				+ TABLE_FILE_EXT);
		return Find.execute(condition);
	}

	/**
	 * 指定したディレクトリに存在するDBMファイルを検索し、DBM名のリストを返します.
	 * <p>
	 * 再帰的に検索は行いません.<br>
	 * 返すDBM名はファイル名から拡張子を除去したものです.
	 * </p>
	 *
	 * @param dir
	 *            対象ディレクトリ
	 * @return 検索されたDBM名のリスト
	 * @throws IOException
	 *             入出力エラーが発生した場合
	 */
	public static List<String> getDbmNameList(String dir) throws IOException {
		FileUtil d = new FileUtil(dir);
		if (!d.isDirectory()) {
			throw new DbmDirNotFoundException(dir);
		}
		// ファイル名一覧を取得し、DBM名一覧を生成する
		List<String> dbmNameList = new ArrayList<String>();
		List<FileUtil> fileList = d.getFileList();
		for (FileUtil file : fileList) {
			String fileName = file.getName();
			if (fileName.endsWith(TABLE_FILE_EXT)) {
				// 拡張子がDBMであればDBM名一覧にセットする
				String dbmName = fileName.substring(0, fileName.length()
						- TABLE_FILE_EXT.length());
				dbmNameList.add(dbmName);
			}
		}
		return dbmNameList;
	}
}
