// SPDX-License-Identifier: GPL-3.0-only

package commslog

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRotationPreservesPriorStreamAndModes(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "events.jsonl")
	prior := []byte("partial prior stream")
	if err := os.WriteFile(path, prior, 0o600); err != nil {
		t.Fatal(err)
	}
	logger := openTestLogger(t, Options{Mode: Content, Path: path, Host: "local", Incarnation: "one",
		MaxFileBytes: minimumFileBytes, MaxFiles: 3, QueueBytes: 1 << 20})
	for index := 0; index < 2; index++ {
		if !logger.Emit(Event{Type: Request, Method: MessageSend, MessageID: string(rune('a' + index)),
			Body: strings.Repeat(string(rune('a'+index)), 3000)}) {
			t.Fatalf("event %d was not admitted", index)
		}
	}
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, path+".2"); string(got) != string(prior) {
		t.Fatalf("prior stream = %q", got)
	}
	if got := decodeRecords(t, mustRead(t, path+".1")); len(got) != 1 || got[0].Sequence != 1 {
		t.Fatalf("first rotation = %+v", got)
	}
	if got := decodeRecords(t, mustRead(t, path)); len(got) != 1 || got[0].Sequence != 2 {
		t.Fatalf("current stream = %+v", got)
	}
	for _, name := range []string{path, path + ".1", path + ".2", path + ".lock"} {
		info, err := os.Lstat(name)
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode = %v", name, info.Mode())
		}
	}
}

func TestOpenRejectsUnsafePathsAndDoesNotChangeDirectoryMode(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{name: "log symlink", setup: func(t *testing.T, path string) {
			target := filepath.Join(filepath.Dir(path), "target")
			if err := os.WriteFile(target, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "log directory", setup: func(t *testing.T, path string) {
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "lock symlink", setup: func(t *testing.T, path string) {
			target := filepath.Join(filepath.Dir(path), "target")
			if err := os.WriteFile(target, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path+".lock"); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "rotation symlink", setup: func(t *testing.T, path string) {
			target := filepath.Join(filepath.Dir(path), "target")
			if err := os.WriteFile(target, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path+".1"); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			if err := os.Chmod(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(directory, "events.jsonl")
			test.setup(t, path)
			_, err := Open(Options{Mode: Metadata, Path: path, Host: "local", Incarnation: "one",
				MaxFileBytes: minimumFileBytes, MaxFiles: 2, QueueBytes: minimumQueueBytes})
			if err == nil {
				t.Fatal("unsafe path was accepted")
			}
		})
	}

	directory := t.TempDir()
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "events.jsonl")
	_, err := Open(Options{Mode: Metadata, Path: path, Host: "local", Incarnation: "one",
		MaxFileBytes: minimumFileBytes, MaxFiles: 1, QueueBytes: minimumQueueBytes})
	if err == nil {
		t.Fatal("public directory was accepted")
	}
	info, statErr := os.Stat(directory)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("directory mode was changed to %v", info.Mode().Perm())
	}
}

func TestExclusiveWriterOwnershipAndRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	options := Options{Mode: Metadata, Path: path, Host: "local", Incarnation: "one",
		MaxFileBytes: minimumFileBytes, MaxFiles: 2, QueueBytes: minimumQueueBytes}
	first, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(options)
	if err == nil {
		_ = second.Close()
		t.Fatal("second writer acquired the same path")
	}
	if !strings.Contains(err.Error(), "already owned") {
		t.Fatalf("second Open error = %v", err)
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := Open(options)
	if err != nil {
		t.Fatalf("lock was not released: %v", err)
	}
	if err = third.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFailedOpenReleasesWriterLock(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "events.jsonl")
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path+".1"); err != nil {
		t.Fatal(err)
	}
	options := Options{Mode: Metadata, Path: path, Host: "local", Incarnation: "one",
		MaxFileBytes: minimumFileBytes, MaxFiles: 2, QueueBytes: minimumQueueBytes}
	if _, err := Open(options); err == nil {
		t.Fatal("unsafe rotation was accepted")
	}
	if err := os.Remove(path + ".1"); err != nil {
		t.Fatal(err)
	}
	logger, err := Open(options)
	if err != nil {
		t.Fatalf("failed Open retained lock: %v", err)
	}
	if err = logger.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseReportsSyncFailureOnce(t *testing.T) {
	want := errors.New("sync failed")
	sink := &failingSink{closeErr: want}
	var callbackCount int
	logger := newLogger(Options{Mode: Metadata, Host: "local", Incarnation: "one",
		MaxFileBytes: 1 << 20, MaxFiles: 1, QueueBytes: 1 << 20,
		OnError: func(error) { callbackCount++ }}, sink)
	if err := logger.Close(); !errors.Is(err, want) {
		t.Fatalf("Close error = %v", err)
	}
	if callbackCount != 1 || logger.Stats().WriteErrors != 1 {
		t.Fatalf("callback=%d stats=%+v", callbackCount, logger.Stats())
	}
}
