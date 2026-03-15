package bitcaskmy

import (
	. "bitcask-my/common"
	"bitcask-my/data"
	"bitcask-my/index"
	"errors"
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
}

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
	//5.构建内存索引
	if err := db.buildIndex(); err != nil {
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
	return nil
	// 创建 logRecord 并写入数据文件
	log_record := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	pos, err := db.appendLogRecord(log_record)
	if err != nil {
		return err
	}
	//更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

//追加写入数据记录到数据文件，并返回记录在文件中的位置（LogRecordPos）

func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
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

// 构建内存索引
func (db *DB) buildIndex() error {
	//1.遍历旧文件集合和活跃文件
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
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}
	//3.跟剧文件ID找到对应的数据文件
	var dataFile *data.DataFile
	if db.aciveFile.FileID == logRecordPos.Fid {
		dataFile = db.aciveFile
	} else {
		dataFile = db.oldfiles[logRecordPos.Fid]
	}
	//4.数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	//5.根据文件偏移读取数据记录
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	//6.判断数据记录类型
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
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
