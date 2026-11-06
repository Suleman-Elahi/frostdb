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

// State holds recovered data ready to be loaded into the engine.
type State struct {
	Records            []checkpoint.Record
	CheckpointMetadata checkpoint.Metadata
	WALEntries         []wal.Entry
}

// Options configures recovery.
type Options struct {
	DataDir       string
	CheckpointDir string
	WALDir        string
}

// Recover loads the latest checkpoint and replays only WAL segments newer than
// its recorded cutoff. Legacy checkpoints use a zero cutoff and preserve the
// historical behavior of replaying every WAL file.
func Recover(opts Options) (*State, error) {
	state := &State{}

	cpDir := opts.CheckpointDir
	if cpDir == "" {
		cpDir = filepath.Join(opts.DataDir, "checkpoint")
	}
	latest, err := checkpoint.Latest(cpDir)
	if err != nil {
		return nil, fmt.Errorf("recovery: find checkpoint: %w", err)
	}
	if latest != "" {
		records, metadata, err := loadCheckpoint(latest)
		if err != nil {
			return nil, fmt.Errorf("recovery: load checkpoint: %w", err)
		}
		state.Records = records
		state.CheckpointMetadata = metadata
	}

	walDir := opts.WALDir
	if walDir == "" {
		walDir = filepath.Join(opts.DataDir, "wal")
	}
	entries, err := replayWAL(walDir, state.CheckpointMetadata.WALCutoff)
	if err != nil {
		return nil, fmt.Errorf("recovery: replay wal: %w", err)
	}
	state.WALEntries = entries
	return state, nil
}

func loadCheckpoint(path string) ([]checkpoint.Record, checkpoint.Metadata, error) {
	reader, err := checkpoint.OpenReader(path)
	if err != nil {
		return nil, checkpoint.Metadata{}, err
	}
	defer reader.Close()

	var records []checkpoint.Record
	for {
		rec, err := reader.ReadRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			return records, reader.Metadata(), err
		}
		records = append(records, rec)
	}
	return records, reader.Metadata(), nil
}

func replayWAL(dir string, cutoff uint64) ([]wal.Entry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var sealed []string
	var current string
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".wal" {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if entry.Name() == "current.wal" {
			current = path
			continue
		}
		if cutoff > 0 {
			sequence, isVersionedSegment := wal.SegmentSequence(entry.Name())
			if !isVersionedSegment || sequence <= cutoff {
				// A FROSTCK2 snapshot includes every pre-cutoff segment. Any
				// legacy rotated file also predates the first versioned cutoff.
				continue
			}
		}
		sealed = append(sealed, path)
	}

	// Replay immutable segments in sequence/name order and current.wal last.
	// The latter may contain writes after the newest sealed segment.
	sort.Strings(sealed)
	if current != "" {
		sealed = append(sealed, current)
	}

	var allEntries []wal.Entry
	for _, path := range sealed {
		fileEntries, err := wal.ReadAll(path)
		if err != nil {
			return allEntries, fmt.Errorf("replay %s: %w", path, err)
		}
		allEntries = append(allEntries, fileEntries...)
	}
	return allEntries, nil
}
