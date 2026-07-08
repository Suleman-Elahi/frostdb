package recovery

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"frostdb/internal/engine/persistence/checkpoint"
	"frostdb/internal/engine/persistence/wal"
)

// State holds recovered data ready to be loaded into the engine
type State struct {
	Records    []checkpoint.Record
	WALEntries []wal.Entry
}

// Options configures recovery
type Options struct {
	DataDir       string
	CheckpointDir string
	WALDir        string
}

// Recover loads the latest checkpoint and replays any WAL entries after it
func Recover(opts Options) (*State, error) {
	state := &State{}

	// Load latest checkpoint
	cpDir := opts.CheckpointDir
	if cpDir == "" {
		cpDir = filepath.Join(opts.DataDir, "checkpoint")
	}

	latest, err := checkpoint.Latest(cpDir)
	if err != nil {
		return nil, fmt.Errorf("recovery: find checkpoint: %w", err)
	}

	if latest != "" {
		records, err := loadCheckpoint(latest)
		if err != nil {
			return nil, fmt.Errorf("recovery: load checkpoint: %w", err)
		}
		state.Records = records
	}

	// Replay WAL
	walDir := opts.WALDir
	if walDir == "" {
		walDir = filepath.Join(opts.DataDir, "wal")
	}

	entries, err := replayWAL(walDir)
	if err != nil {
		return nil, fmt.Errorf("recovery: replay wal: %w", err)
	}
	state.WALEntries = entries

	return state, nil
}

func loadCheckpoint(path string) ([]checkpoint.Record, error) {
	reader, err := checkpoint.OpenReader(path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var records []checkpoint.Record
	for {
		rec, err := reader.ReadRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			return records, err
		}
		records = append(records, rec)
	}
	return records, nil
}

func replayWAL(dir string) ([]wal.Entry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	// Sort WAL files by name (timestamp-based)
	var walFiles []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".wal" {
			walFiles = append(walFiles, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(walFiles)

	var allEntries []wal.Entry
	for _, path := range walFiles {
		fileEntries, err := wal.ReadAll(path)
		if err != nil {
			return allEntries, fmt.Errorf("replay %s: %w", path, err)
		}
		allEntries = append(allEntries, fileEntries...)
	}

	return allEntries, nil
}
