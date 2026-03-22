package fio

const (
	DataFilePerm = 0644
)

type IOManager interface {
	//从文件指定位置读,返回读取的字节数和错误信息
	ReadAt(p []byte, off int64) (n int, err error)
	//向文件指定位置写,返回写入的字节数和错误信息
	WriteAt(p []byte) (n int, err error)
	//将内存中的数据刷新到磁盘,确保数据持久化
	Sync() error
	//关闭文件,释放资源
	Close() error
	// Size 返回文件的当前大小
	Size() (int64, error)
}

// NewIOManager 创建一个新的 IOManager 实例，负责管理文件的读写操作。
func NewIOManager(filePath string) (IOManager, error) {
	return NewFileIO(filePath)
}
