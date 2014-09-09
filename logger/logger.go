package logger

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

//Loglevels
const (
    ERR = 0
    INFO = 1
    DEBUG = 2
)

type rLogger struct {
    errFp *os.File
    infoFp *os.File
    debug *os.File
}

//Dummy debugging 
func New(errFile string, infoFile string, debugFile string ) (l *rLogger) {
    if fp, err := os.Create(errFile); err != nil {
        log.Errorf("Unable to create log files %s", err)
    }

	return
}

//Closes all the log files
func (l *Logger) close() {
	if l.w != nil {
		l.w.Close()
		l.w = nil
	}
}

// SetPrefix changes the prefix for an already created logger
func (l *Logger) SetPrefix(p string) {
	l.prefix = "[" + p + "] "
}

// Close closes the logger and the syslog connection
func (l *Logger) Close() {
	l.terminate <- true
	<-l.terminate
	close(l.terminate)
	l.close()
}

// printf is the internal implementation of Printf used by Printf and
// CoalesceF to format the log message. Returns a formatted string and
// a boolean indicating whether logging should happen.
func (l *Logger) printf(level uint, format string, v ...interface{}) (string, bool) {
	if (l.w != nil || Addr == "") && (l.all || level <= LogLevel) {
		return fmt.Sprintf(l.prefix+format, v...), true
	} else {
		return "", false
	}
}

// write is used to send a formatted message with a specific level to
// the appropriate syslog method. If log is false this function does
// nothing.
func (l *Logger) write(level uint, m string, log bool) {
	if log {
		if Addr != "" {
			switch level {
			case LevelError:
				_ = l.w.Err(m)
			case LevelInfo:
				_ = l.w.Info(m)
			case LevelInternal, LevelDebug:
				_ = l.w.Debug(m)
			}
		} else {
			priority := syslog.LOG_EMERG
			switch level {
			case LevelError:
				priority = syslog.LOG_ERR
			case LevelInfo:
				priority = syslog.LOG_INFO
			case LevelInternal, LevelDebug:
				priority = syslog.LOG_DEBUG
			}

			s := C.CString(m)
			C.csyslog(C.int(priority), s)
			C.free(unsafe.Pointer(s))
		}

		l.Logged = m
	} else {
		l.Logged = ""
	}
}

// Printf is like fmt.Printf but goes to the logger if the log level
// is correct
func (l *Logger) Printf(level uint, format string, v ...interface{}) {
	m, log := l.printf(level, format, v...)
	l.write(level, m, log)
}


