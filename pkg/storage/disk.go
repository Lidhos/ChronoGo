package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"ChronoGo/pkg/model"
	"ChronoGo/pkg/util"
)

const (
	// 文件头魔数
	fileHeaderMagic uint32 = 0x43474F44 // "CGOD" in hex

	// 文件版本
	fileVersion uint32 = 1

	// 块头魔数
	blockHeaderMagic uint32 = 0x43474F42 // "CGOB" in hex

	// 最大块大小
	maxBlockSize int64 = 64 * 1024 * 1024 // 64MB

	// 文件头大小
	fileHeaderSize int64 = 8
)

// FileHeader 数据文件头
type FileHeader struct {
	Magic   uint32 // 魔数
	Version uint32 // 版本号
}

// BlockHeader 数据块头
type BlockHeader struct {
	Magic      uint32 // 魔数
	Size       uint32 // 块大小
	StartTime  int64  // 块内最小时间戳
	EndTime    int64  // 块内最大时间戳
	PointCount uint32 // 数据点数量
	Checksum   uint32 // 校验和
}

// DataFile 数据文件
type DataFile struct {
	path       string       // 文件路径
	file       *os.File     // 文件句柄
	size       int64        // 文件大小
	blocks     []*BlockInfo // 块信息
	mu         sync.RWMutex // 读写锁
	writable   bool         // 是否可写
	startTime  int64        // 文件内最小时间戳
	endTime    int64        // 文件内最大时间戳
	pointCount uint32       // 数据点数量
}

// BlockInfo 块信息
type BlockInfo struct {
	Offset int64       // 块在文件中的偏移量
	Header BlockHeader // 块头
}

// NewDataFile 创建新的数据文件
func NewDataFile(path string, writable bool) (*DataFile, error) {
	var file *os.File
	var err error
	var fileSize int64

	// 确保目录存在
	dir := filepath.Dir(path)
	if err = os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	if writable {
		// 打开可写文件
		file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open data file: %w", err)
		}

		// 获取文件大小
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to stat data file: %w", err)
		}
		fileSize = info.Size()

		// 如果是新文件，写入文件头
		if fileSize == 0 {
			header := FileHeader{
				Magic:   fileHeaderMagic,
				Version: fileVersion,
			}

			// 写入文件头
			if err := binary.Write(file, binary.LittleEndian, &header); err != nil {
				file.Close()
				return nil, fmt.Errorf("failed to write file header: %w", err)
			}

			fileSize = fileHeaderSize
		}
	} else {
		// 打开只读文件
		file, err = os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("failed to open data file: %w", err)
		}

		// 获取文件大小
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to stat data file: %w", err)
		}
		fileSize = info.Size()
	}

	df := &DataFile{
		path:     path,
		file:     file,
		size:     fileSize,
		blocks:   make([]*BlockInfo, 0),
		writable: writable,
	}

	// 加载块信息
	if err := df.loadBlocks(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to load blocks: %w", err)
	}

	return df, nil
}

// Close 关闭数据文件
func (df *DataFile) Close() error {
	df.mu.Lock()
	defer df.mu.Unlock()

	if df.file != nil {
		if err := df.file.Close(); err != nil {
			return fmt.Errorf("failed to close data file: %w", err)
		}
		df.file = nil
	}
	return nil
}

// loadBlocks 加载块信息
func (df *DataFile) loadBlocks() error {
	df.mu.Lock()
	defer df.mu.Unlock()

	// 重置文件指针到文件头之后
	if _, err := df.file.Seek(fileHeaderSize, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to file header: %w", err)
	}

	// 读取所有块
	offset := fileHeaderSize
	for offset < df.size {
		// 读取块头
		var header BlockHeader
		if err := binary.Read(df.file, binary.LittleEndian, &header); err != nil {
			return fmt.Errorf("failed to read block header at offset %d: %w", offset, err)
		}

		// 验证块头魔数
		if header.Magic != blockHeaderMagic {
			return fmt.Errorf("invalid block header magic at offset %d", offset)
		}

		// 添加块信息
		df.blocks = append(df.blocks, &BlockInfo{
			Offset: offset,
			Header: header,
		})

		// 更新文件时间范围
		if df.startTime == 0 || header.StartTime < df.startTime {
			df.startTime = header.StartTime
		}
		if header.EndTime > df.endTime {
			df.endTime = header.EndTime
		}

		// 更新数据点数量
		df.pointCount += header.PointCount

		// 移动到下一个块
		offset += int64(header.Size) + int64(binary.Size(header))
		if _, err := df.file.Seek(offset, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek to next block: %w", err)
		}
	}

	return nil
}

// WriteBlock 写入数据块
func (df *DataFile) WriteBlock(block *DataBlock) error {
	if !df.writable {
		return fmt.Errorf("data file is not writable")
	}

	df.mu.Lock()
	defer df.mu.Unlock()

	// 序列化块数据
	data, err := block.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize block: %w", err)
	}

	// 计算校验和
	checksum := util.CRC32(data)

	// 创建块头
	header := BlockHeader{
		Magic:      blockHeaderMagic,
		Size:       uint32(len(data)),
		StartTime:  block.StartTime,
		EndTime:    block.EndTime,
		PointCount: uint32(block.PointCount),
		Checksum:   checksum,
	}

	// 将文件指针移动到文件末尾
	if _, err := df.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek to end of file: %w", err)
	}

	// 获取当前偏移量
	offset, err := df.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get current offset: %w", err)
	}

	// 写入块头
	if err := binary.Write(df.file, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("failed to write block header: %w", err)
	}

	// 写入块数据
	if _, err := df.file.Write(data); err != nil {
		return fmt.Errorf("failed to write block data: %w", err)
	}

	// 添加块信息
	df.blocks = append(df.blocks, &BlockInfo{
		Offset: offset,
		Header: header,
	})

	// 更新文件大小
	df.size = offset + int64(binary.Size(header)) + int64(len(data))

	// 更新文件时间范围
	if df.startTime == 0 || header.StartTime < df.startTime {
		df.startTime = header.StartTime
	}
	if header.EndTime > df.endTime {
		df.endTime = header.EndTime
	}

	// 更新数据点数量
	df.pointCount += header.PointCount

	// 同步文件到磁盘
	return df.file.Sync()
}

// ReadBlock 读取数据块
func (df *DataFile) ReadBlock(index int) (*DataBlock, error) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	if index < 0 || index >= len(df.blocks) {
		return nil, fmt.Errorf("block index out of range: %d", index)
	}

	blockInfo := df.blocks[index]
	headerSize := int64(binary.Size(BlockHeader{}))

	// 移动到块数据开始位置
	if _, err := df.file.Seek(blockInfo.Offset+headerSize, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to block data: %w", err)
	}

	// 读取块数据
	data := make([]byte, blockInfo.Header.Size)
	if _, err := io.ReadFull(df.file, data); err != nil {
		return nil, fmt.Errorf("failed to read block data: %w", err)
	}

	// 验证校验和
	checksum := util.CRC32(data)
	if checksum != blockInfo.Header.Checksum {
		return nil, fmt.Errorf("block checksum mismatch")
	}

	// 反序列化块数据
	block := &DataBlock{
		StartTime:  blockInfo.Header.StartTime,
		EndTime:    blockInfo.Header.EndTime,
		PointCount: int(blockInfo.Header.PointCount),
	}
	if err := block.Deserialize(data); err != nil {
		return nil, fmt.Errorf("failed to deserialize block: %w", err)
	}

	return block, nil
}

// FindBlocksByTimeRange 根据时间范围查找块
func (df *DataFile) FindBlocksByTimeRange(startTime, endTime int64) []int {
	df.mu.RLock()
	defer df.mu.RUnlock()

	var result []int
	for i, block := range df.blocks {
		// 如果块的时间范围与查询范围有交集
		if block.Header.EndTime >= startTime && block.Header.StartTime <= endTime {
			result = append(result, i)
		}
	}
	return result
}

// GetTimeRange 获取文件的时间范围
func (df *DataFile) GetTimeRange() (int64, int64) {
	df.mu.RLock()
	defer df.mu.RUnlock()
	return df.startTime, df.endTime
}

// GetPointCount 获取文件中的数据点数量
func (df *DataFile) GetPointCount() uint32 {
	df.mu.RLock()
	defer df.mu.RUnlock()
	return df.pointCount
}

// DataBlock 数据块
type DataBlock struct {
	StartTime  int64              // 块内最小时间戳
	EndTime    int64              // 块内最大时间戳
	PointCount int                // 数据点数量
	Columns    map[string]*Column // 列数据
}

// NewDataBlock 创建新的数据块
func NewDataBlock() *DataBlock {
	return &DataBlock{
		Columns: make(map[string]*Column),
	}
}

// AddPoint 添加数据点
func (db *DataBlock) AddPoint(point *model.TimeSeriesPoint) error {
	// 更新时间范围
	timestamp := point.Timestamp
	if db.StartTime == 0 || timestamp < db.StartTime {
		db.StartTime = timestamp
	}
	if timestamp > db.EndTime {
		db.EndTime = timestamp
	}

	// 添加时间戳
	timeCol, ok := db.Columns["timestamp"]
	if !ok {
		col := NewColumn("timestamp", ColumnTypeTimestamp)
		db.Columns["timestamp"] = col
		timeCol = col
	}
	if err := timeCol.AddValue(timestamp); err != nil {
		return fmt.Errorf("failed to add timestamp: %w", err)
	}

	// 添加标签
	for key, value := range point.Tags {
		colName := "tags." + key
		col, ok := db.Columns[colName]
		if !ok {
			col = NewColumn(colName, ColumnTypeString)
			db.Columns[colName] = col
		}
		if err := col.AddValue(value); err != nil {
			return fmt.Errorf("failed to add tag %s: %w", key, err)
		}
	}

	// 添加字段
	for key, value := range point.Fields {
		colName := "fields." + key
		col, ok := db.Columns[colName]
		if !ok {
			// 根据值类型创建适当的列
			var colType byte
			switch v := value.(type) {
			case float64:
				colType = ColumnTypeFloat
			case int64:
				colType = ColumnTypeInteger
			case string:
				colType = ColumnTypeString
			case bool:
				colType = ColumnTypeBoolean
			default:
				return fmt.Errorf("unsupported field type for %s: %T", key, v)
			}
			col = NewColumn(colName, colType)
			db.Columns[colName] = col
		}
		if err := col.AddValue(value); err != nil {
			return fmt.Errorf("failed to add field %s: %w", key, err)
		}
	}

	// 更新数据点计数
	db.PointCount++

	return nil
}

// Serialize 序列化数据块
func (db *DataBlock) Serialize() ([]byte, error) {
	// 创建缓冲区
	buffer := util.NewBuffer(nil)

	// 写入列数量
	buffer.WriteUint32(uint32(len(db.Columns)))

	// 写入每一列
	for name, col := range db.Columns {
		// 写入列名
		buffer.WriteString(name)

		// 写入列类型
		buffer.WriteUint8(col.Type)

		// 序列化列数据
		colData, err := col.Serialize()
		if err != nil {
			return nil, fmt.Errorf("failed to serialize column %s: %w", name, err)
		}

		// 写入列数据长度
		buffer.WriteUint32(uint32(len(colData)))

		// 写入列数据
		buffer.Write(colData)
	}

	return buffer.Bytes(), nil
}

// Deserialize 反序列化数据块
func (db *DataBlock) Deserialize(data []byte) error {
	// 创建缓冲区
	buffer := util.NewBuffer(data)

	// 读取列数量
	colCount, err := buffer.ReadUint32()
	if err != nil {
		return fmt.Errorf("failed to read column count: %w", err)
	}

	// 初始化列映射
	db.Columns = make(map[string]*Column)

	// 读取每一列
	for i := uint32(0); i < colCount; i++ {
		// 读取列名
		name, err := buffer.ReadString()
		if err != nil {
			return fmt.Errorf("failed to read column name: %w", err)
		}

		// 读取列类型
		colType, err := buffer.ReadUint8()
		if err != nil {
			return fmt.Errorf("failed to read column type: %w", err)
		}

		// 读取列数据长度
		colDataLen, err := buffer.ReadUint32()
		if err != nil {
			return fmt.Errorf("failed to read column data length: %w", err)
		}

		// 读取列数据
		colData, err := buffer.ReadBytes(int(colDataLen))
		if err != nil {
			return fmt.Errorf("failed to read column data: %w", err)
		}

		// 创建列
		col := NewColumn(name, colType)

		// 反序列化列数据
		if err := col.Deserialize(colData); err != nil {
			return fmt.Errorf("failed to deserialize column %s: %w", name, err)
		}

		// 添加列
		db.Columns[name] = col
	}

	return nil
}

// GetPoints 获取数据点
func (db *DataBlock) GetPoints() ([]*model.TimeSeriesPoint, error) {
	// 获取时间戳列
	timeCol, ok := db.Columns["timestamp"]
	if !ok {
		return nil, fmt.Errorf("timestamp column not found")
	}

	// 获取时间戳数量
	count := timeCol.Count
	if count == 0 {
		return nil, nil
	}

	// 创建结果数组
	points := make([]*model.TimeSeriesPoint, count)

	// 初始化每个数据点
	for i := 0; i < count; i++ {
		points[i] = &model.TimeSeriesPoint{
			Tags:   make(map[string]string),
			Fields: make(map[string]interface{}),
		}
	}

	// 填充时间戳
	for i := 0; i < count; i++ {
		timestamp, err := timeCol.GetValue(i)
		if err != nil {
			return nil, fmt.Errorf("failed to get timestamp at index %d: %w", i, err)
		}
		points[i].Timestamp = timestamp.(int64)
	}

	// 填充标签和字段
	for name, col := range db.Columns {
		if name == "timestamp" {
			continue
		}

		// 解析列名
		parts := util.SplitColumnName(name)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid column name: %s", name)
		}

		category, key := parts[0], parts[1]

		// 填充数据
		for i := 0; i < count; i++ {
			value, err := col.GetValue(i)
			if err != nil {
				return nil, fmt.Errorf("failed to get value at index %d for column %s: %w", i, name, err)
			}

			if category == "tags" {
				points[i].Tags[key] = value.(string)
			} else if category == "fields" {
				points[i].Fields[key] = value
			}
		}
	}

	return points, nil
}

// TableDataFile 表数据文件管理
type TableDataFile struct {
	basePath    string               // 基础路径
	table       string               // 表名
	database    string               // 数据库名
	dataFiles   []*DataFile          // 数据文件列表
	filesByTime map[string]*DataFile // 按时间分片的文件映射
	mu          sync.RWMutex         // 读写锁
}

// NewTableDataFile 创建表数据文件管理器
func NewTableDataFile(basePath, database, table string) (*TableDataFile, error) {
	tdf := &TableDataFile{
		basePath:    basePath,
		table:       table,
		database:    database,
		dataFiles:   make([]*DataFile, 0),
		filesByTime: make(map[string]*DataFile),
	}

	// 加载现有数据文件
	if err := tdf.loadDataFiles(); err != nil {
		return nil, err
	}

	return tdf, nil
}

// loadDataFiles 加载现有数据文件
func (tdf *TableDataFile) loadDataFiles() error {
	tdf.mu.Lock()
	defer tdf.mu.Unlock()

	// 构建表数据目录路径
	tablePath := filepath.Join(tdf.basePath, tdf.database, tdf.table)

	// 检查目录是否存在
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		// 目录不存在，创建它
		if err := os.MkdirAll(tablePath, 0755); err != nil {
			return fmt.Errorf("failed to create table directory: %w", err)
		}
		return nil
	}

	// 读取目录中的所有文件
	files, err := os.ReadDir(tablePath)
	if err != nil {
		return fmt.Errorf("failed to read table directory: %w", err)
	}

	// 加载每个数据文件
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		// 检查是否是数据文件
		if filepath.Ext(file.Name()) != ".data" {
			continue
		}

		// 打开数据文件
		filePath := filepath.Join(tablePath, file.Name())
		dataFile, err := NewDataFile(filePath, false)
		if err != nil {
			return fmt.Errorf("failed to open data file %s: %w", filePath, err)
		}

		// 添加到文件列表
		tdf.dataFiles = append(tdf.dataFiles, dataFile)

		// 添加到时间映射
		timeKey := filepath.Base(file.Name())
		timeKey = timeKey[:len(timeKey)-5] // 去掉 .data 后缀
		tdf.filesByTime[timeKey] = dataFile
	}

	return nil
}

// Close 关闭所有数据文件
func (tdf *TableDataFile) Close() error {
	tdf.mu.Lock()
	defer tdf.mu.Unlock()

	var lastErr error
	for _, file := range tdf.dataFiles {
		if err := file.Close(); err != nil {
			lastErr = err
		}
	}

	tdf.dataFiles = nil
	tdf.filesByTime = make(map[string]*DataFile)

	return lastErr
}

// GetDataFileForTime 获取指定时间的数据文件
func (tdf *TableDataFile) GetDataFileForTime(timestamp int64) (*DataFile, error) {
	tdf.mu.RLock()
	defer tdf.mu.RUnlock()

	// 生成时间分片键
	timeKey := formatTimeKey(timestamp)

	// 检查是否已有该时间分片的文件
	if file, ok := tdf.filesByTime[timeKey]; ok {
		return file, nil
	}

	// 没有找到，需要创建新文件
	tdf.mu.RUnlock()
	tdf.mu.Lock()
	defer tdf.mu.Unlock()
	tdf.mu.RLock()

	// 再次检查，防止在锁切换期间被其他goroutine创建
	if file, ok := tdf.filesByTime[timeKey]; ok {
		return file, nil
	}

	// 创建新文件
	filePath := filepath.Join(tdf.basePath, tdf.database, tdf.table, timeKey+".data")
	dataFile, err := NewDataFile(filePath, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create data file %s: %w", filePath, err)
	}

	// 添加到文件列表和映射
	tdf.dataFiles = append(tdf.dataFiles, dataFile)
	tdf.filesByTime[timeKey] = dataFile

	return dataFile, nil
}

// FindDataFilesByTimeRange 根据时间范围查找数据文件
func (tdf *TableDataFile) FindDataFilesByTimeRange(startTime, endTime int64) []*DataFile {
	tdf.mu.RLock()
	defer tdf.mu.RUnlock()

	var result []*DataFile
	for _, file := range tdf.dataFiles {
		fileStart, fileEnd := file.GetTimeRange()
		// 如果文件的时间范围与查询范围有交集
		if fileEnd >= startTime && fileStart <= endTime {
			result = append(result, file)
		}
	}
	return result
}

// WritePoints 写入数据点
func (tdf *TableDataFile) WritePoints(points []*model.TimeSeriesPoint) error {
	if len(points) == 0 {
		return nil
	}

	// 按时间分片对数据点进行分组
	pointsByTime := make(map[string][]*model.TimeSeriesPoint)
	for _, point := range points {
		timeKey := formatTimeKey(point.Timestamp)
		pointsByTime[timeKey] = append(pointsByTime[timeKey], point)
	}

	// 对每个时间分片写入数据
	for timeKey, timePoints := range pointsByTime {
		// 获取或创建数据文件
		dataFile, err := tdf.GetDataFileForTime(timePoints[0].Timestamp)
		if err != nil {
			return fmt.Errorf("failed to get data file for time %s: %w", timeKey, err)
		}

		// 创建数据块
		block := NewDataBlock()
		for _, point := range timePoints {
			if err := block.AddPoint(point); err != nil {
				return fmt.Errorf("failed to add point to block: %w", err)
			}
		}

		// 写入数据块
		if err := dataFile.WriteBlock(block); err != nil {
			return fmt.Errorf("failed to write block: %w", err)
		}
	}

	return nil
}

// QueryPoints 查询数据点
func (tdf *TableDataFile) QueryPoints(startTime, endTime int64, tagFilters map[string]string) ([]*model.TimeSeriesPoint, error) {
	// 查找时间范围内的数据文件
	dataFiles := tdf.FindDataFilesByTimeRange(startTime, endTime)
	if len(dataFiles) == 0 {
		return nil, nil
	}

	var allPoints []*model.TimeSeriesPoint

	// 从每个数据文件中查询数据点
	for _, dataFile := range dataFiles {
		// 查找时间范围内的块
		blockIndices := dataFile.FindBlocksByTimeRange(startTime, endTime)
		if len(blockIndices) == 0 {
			continue
		}

		// 读取每个块中的数据点
		for _, blockIndex := range blockIndices {
			block, err := dataFile.ReadBlock(blockIndex)
			if err != nil {
				return nil, fmt.Errorf("failed to read block %d: %w", blockIndex, err)
			}

			// 获取块中的所有数据点
			points, err := block.GetPoints()
			if err != nil {
				return nil, fmt.Errorf("failed to get points from block: %w", err)
			}

			// 过滤数据点
			for _, point := range points {
				// 检查时间范围
				if point.Timestamp < startTime || point.Timestamp > endTime {
					continue
				}

				// 检查标签过滤器
				match := true
				for key, value := range tagFilters {
					if point.Tags[key] != value {
						match = false
						break
					}
				}

				if match {
					allPoints = append(allPoints, point)
				}
			}
		}
	}

	return allPoints, nil
}

// formatTimeKey 格式化时间键
func formatTimeKey(timestamp int64) string {
	// 转换为时间
	t := time.Unix(0, timestamp)
	// 格式化为 YYYYMMDD 格式
	return t.Format("20060102")
}
