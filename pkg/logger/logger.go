package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	// 全局日志记录器
	globalLogger *Logger
	// 互斥锁，用于保护全局日志记录器
	mu sync.Mutex
	// 是否启用日志
	enabled bool
)

// Logger 日志管理器
type Logger struct {
	enabled bool
	logFile *os.File
	logger  *log.Logger
}

// Init 初始化日志系统
func Init(logEnabled bool, logDir string) error {
	mu.Lock()
	defer mu.Unlock()

	enabled = logEnabled

	if globalLogger != nil {
		// 关闭现有的日志文件
		if globalLogger.logFile != nil {
			globalLogger.logFile.Close()
		}
	}

	logger := &Logger{
		enabled: logEnabled,
	}

	if logEnabled {
		// 创建日志目录
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return fmt.Errorf("创建日志目录失败: %v", err)
		}

		// 创建日志文件，使用当前时间作为文件名
		logFileName := filepath.Join(logDir, time.Now().Format("2006-01-02_15-04-05")+".log")
		logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("创建日志文件失败: %v", err)
		}

		// 设置日志输出到文件和控制台
		multiWriter := io.MultiWriter(os.Stdout, logFile)
		logger.logger = log.New(multiWriter, "", log.LstdFlags)
		logger.logFile = logFile
	} else {
		// 仅输出到控制台
		logger.logger = log.New(os.Stdout, "", log.LstdFlags)
	}

	globalLogger = logger
	return nil
}

// Close 关闭日志系统
func Close() {
	mu.Lock()
	defer mu.Unlock()

	if globalLogger != nil && globalLogger.logFile != nil {
		globalLogger.logFile.Close()
		globalLogger.logFile = nil
	}
}

// Printf 打印格式化日志
func Printf(format string, v ...interface{}) {
	mu.Lock()
	defer mu.Unlock()

	if !enabled {
		return
	}

	if globalLogger != nil && globalLogger.logger != nil {
		globalLogger.logger.Printf(format, v...)
	} else {
		fmt.Printf(format, v...)
	}
}

// Println 打印行日志
func Println(v ...interface{}) {
	mu.Lock()
	defer mu.Unlock()

	if !enabled {
		return
	}

	if globalLogger != nil && globalLogger.logger != nil {
		globalLogger.logger.Println(v...)
	} else {
		fmt.Println(v...)
	}
}

// Fatalf 打印致命错误并退出
func Fatalf(format string, v ...interface{}) {
	mu.Lock()
	defer mu.Unlock()

	// 即使日志被禁用，致命错误也应该输出并导致程序退出
	if globalLogger != nil && globalLogger.logger != nil {
		globalLogger.logger.Fatalf(format, v...)
	} else {
		log.Fatalf(format, v...)
	}
}

// IsEnabled 返回日志是否启用
func IsEnabled() bool {
	mu.Lock()
	defer mu.Unlock()
	return enabled
}
