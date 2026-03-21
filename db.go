package bitcaskmy

import (
	. "bitcask-my/common"
	"bitcask-my/data"
	"bitcask-my/index"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	options   Options
	mu        *sync.RWMutex
	fileIds   []int                     //数据文件ID列表,只能在加载索引时使用
	aciveFile *data.DataFile            //活跃文件
	oldfiles  map[uint32]*data.DataFile //旧文件
	index     index.Indexer             //内存索引
	seqNo     uint64                    //事务的序列号 用于实现事务的原子性和一致性
}

// Open 打开或创建一个 Bitcask 数据库实例，加载数据文件并构建内存索引。
func Open(options Options) (*DB, error) {
	//1.配置校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	//2. 目录校验
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//3.创建DB实例
	db := &DB{
		options:  options,
		mu:       &sync.RWMutex{},
		oldfiles: make(map[uint32]*data.DataFile),
		index:    index.NewIndexer(BTreeIndex),
	}
	//4.加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}
	//5.从数据文件加载索引到内存
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

// 写入 key/value 数据 ,key 不能为空
func (db *DB) Put(key, value []byte) error {
	//判断key是否有效
	if len(key) == 0 {
		return ErrKeyNotFound
	}
	// 创建 logRecord 并写入数据文件
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	//更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// ListKeys 列出数据库中所有的 key，返回一个包含所有 key 的切片。
func (db *DB) ListKeys() ([][]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var keys [][]byte
	iter := db.index.Iterator(false)
	defer iter.Close()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		keys = append(keys, iter.Key())
	}
	return keys, nil
}

// Fold 获取所有的数据 并执行用户指定的操作
func (db *DB) Fold(fn func(key, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	iter := db.index.Iterator(false)
	defer iter.Close()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		value, err := db.getValueByPos(iter.Value())
		if err != nil {
			return err
		}
		if !fn(iter.Key(), value) {
			break
		}
	}
	return nil
}

// getValueByPos 根据给定的 LogRecordPos 从数据文件中读取对应的 value，并处理相关错误。
func (db *DB) getValueByPos(logRecordPos *data.LogRecordPos) ([]byte, error) {
	//1.判断数据位置是否有效
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}
	//2.跟剧文件ID找到对应的数据文件
	var dataFile *data.DataFile
	if db.aciveFile.FileID == logRecordPos.Fid {
		dataFile = db.aciveFile
	} else {
		dataFile = db.oldfiles[logRecordPos.Fid]
	}
	//3.数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	//4.根据文件偏移读取数据记录
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	//5.判断数据记录类型
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

// 追加写入数据记录到数据文件，并返回记录在文件中的位置（LogRecordPos）
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	//判断活跃文件是否存在
	if db.aciveFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	//将 logRecord 序列化并写入活跃文件
	encodedStr, size := data.EncodeLogRecord(logRecord)
	//1.写入数据文件
	//1.1判断是否超出文件
	//如果超出则新文件变旧文件 关闭活跃文件 并打开新文件
	if db.aciveFile.WriteOff+size > db.options.DataFileSize {
		//将活跃文件持久化
		if err := db.aciveFile.Sync(); err != nil {
			return nil, err
		}

		db.oldfiles[db.aciveFile.FileID] = db.aciveFile
		//打开新文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	//writeOff 是当前文件的写入偏移，记录了新数据在文件中的位置
	writeOff := db.aciveFile.WriteOff

	//写入数据文件
	if err := db.aciveFile.WriteAt(encodedStr, writeOff); err != nil {
		return nil, err
	}
	// 如果配置了 SyncWrites，则在每次写入后立即将数据刷新到磁盘
	if db.options.SyncWrites {
		if err := db.aciveFile.Sync(); err != nil {
			return nil, err
		}
	}
	pos := &data.LogRecordPos{
		Fid:    db.aciveFile.FileID,
		Offset: writeOff,
	}
	return pos, nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 设置当前活跃文件
// 共享锁保护，确保并发安全
func (db *DB) setActiveDataFile() error {
	//创建新的活跃文件，并将其设置为 db.aciveFile
	var initialFileId uint32 = 0
	if db.aciveFile != nil {
		initialFileId = db.aciveFile.FileID + 1
	}
	//打开新的数据文件
	datafile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.aciveFile = datafile
	return nil
}

// 从数据文件加载索引到内存
// 遍历文件中的所有记录 并更新到内存索引中去
func (db *DB) loadIndexFromDataFiles() error {
	//1.没有文件, 数据库为空 直接返回
	if len(db.fileIds) == 0 {
		return nil
	}
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) error {
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at Start")
		}
		return nil
	}
	// 暂存事务记录，等事务完成后再更新索引
	TransactionRecords := make(map[uint64][]*data.TransactionRecord)

	var currentSeqNo uint64 = nonTransactionSeqNo

	//2.遍历所有文件Id,处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if db.aciveFile.FileID == fileId {
			dataFile = db.aciveFile
		} else {
			dataFile = db.oldfiles[fileId]
		}
		if dataFile == nil {
			return ErrDataFileNotFound
		}
		// 2.1循环处理文件中的数据
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				//如果读取到文件末尾，跳出循环
				if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) {
					break
				}
				return err
			}
			//2.1.1更新内存索引
			pos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}
			//2.1.2 解析key ,拿到实物序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// 如果是非事务记录，直接更新索引
				updateIndex(realKey, logRecord.Type, pos)
			} else {
				// 事务完成,对应的seqno 的数据可以更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, record := range TransactionRecords[seqNo] {
						updateIndex(record.Record.Key, record.Record.Type, record.Pos)
					}
					delete(TransactionRecords, seqNo)
				} else {
					// 否则暂存事务记录
					TransactionRecords[seqNo] = append(TransactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    pos,
					})
				}
			}
			// 更新当前最大的事务序列号，确保后续的事务记录能够正确解析
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}
			offset += size
		}
		//2.2 如果是活跃文件，更新写入偏移
		if i == len(db.fileIds)-1 {
			db.aciveFile.WriteOff = offset
		}
	}
	//更新事务序列号
	db.seqNo = currentSeqNo
	return nil
}

// 加载数据文件
func (db *DB) loadDataFiles() error {
	//1.扫描数据目录，找到所有数据文件
	files, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	//2.遍历所有的文件 找到所有以.data结尾的文件，并解析出文件ID

	for _, file := range files {
		if strings.HasSuffix(file.Name(), DataFileSuffix) {
			//2.1解析文件ID
			splitNames := strings.Split(file.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}

	}
	//3.根据文件ID排序，确保旧文件按顺序加载
	sort.Ints(fileIds)
	db.fileIds = fileIds
	//4.加载数据文件
	for i, fileId := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId))
		if err != nil {
			return err
		}
		//4.1 如果是最后一个文件，则设置为活跃文件，否则放入旧文件集合
		if i == len(fileIds)-1 {
			db.aciveFile = dataFile
		} else {
			db.oldfiles[uint32(fileId)] = dataFile
		}
	}
	return nil
}

// 根据key读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	//1.判断key是否有效
	if len(key) == 0 {
		return nil, ErrKeyNotFound
	}
	//2.从内存数据结构中拿出 key对应的索引信息
	logRecordPos := db.index.Get(key)
	return db.getValueByPos(logRecordPos)
}

// 删除数据
func (db *DB) Delete(key []byte) error {
	//1.判断key是否有效
	if len(key) == 0 {
		return ErrKeyNotFound
	}
	//2.查找key 不加判断会导致删除无效key时，写入一条无效记录到数据文件中，浪费存储空间
	if pos := db.index.Get(key); pos == nil {
		return nil
	}
	//3.创建删除记录并写入数据文件
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}
	if _, err := db.appendLogRecordWithLock(logRecord); err != nil {
		return err
	}
	//4.更新内存索引
	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// 可以使用 go-playground/validator 进行更复杂的配置校验
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("data directory path cannot be empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("data file size must be greater than zero")
	}
	return nil
}

// 解析 logRecord 的 key，提取出实际的 key 和事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
