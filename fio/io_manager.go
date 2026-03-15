package fio

const (
	DataFilePerm = 0644
)

type IOManager interface {
	//从文件指定位置读,返回读取的字节数和错误信息
	ReadAt(p []byte, off int64) (n int, err error)
	//向文件指定位置写,返回写入的字节数和错误信息
	WriteAt(p []byte, off int64) error

	//将内存中的数据刷新到磁盘,确保数据持久化
	Sync() error
	Close() error
}
