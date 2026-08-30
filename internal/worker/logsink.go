package worker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type logLine struct {
	At      time.Time       `json:"at"`
	TaskID  string          `json:"task_id"`
	Attempt int             `json:"attempt"`
	Payload json.RawMessage `json:"payload"`
}

type LogSink struct {
	file    *os.File
	writer  *bufio.Writer
	lines   chan logLine
	stopped chan struct{}
}

func NewLogSink(path string) (*LogSink, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("could not open %s: %w", path, err)
	}

	sink := &LogSink{
		file:    file,
		writer:  bufio.NewWriter(file),
		lines:   make(chan logLine, 128),
		stopped: make(chan struct{}),
	}
	go sink.run()

	return sink, err
}

func (l *LogSink) run() {
	defer close(l.stopped)

	for line := range l.lines {
		encoded, err := json.Marshal(line)
		if err != nil {
			continue
		}
		_, _ = l.writer.Write(encoded)
		_ = l.writer.WriteByte('\n')
	}
}

func (l *LogSink) Append(line logLine) {
	l.lines <- line
}

func (l *LogSink) Close() error {
	close(l.lines)
	<-l.stopped 

	if err := l.writer.Flush(); err != nil {
		_ = l.file.Close()
		return err
	}

	return l.file.Close()
}