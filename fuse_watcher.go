package main

import (
	"io"
	"io/fs"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
)

// FuseWatcher watches a FUSE mount point and consumes files written to it
type FuseWatcher struct {
	mountPath       string
	server          *fuse.Server
	entries         chan fs.DirEntry
	mu              sync.Mutex
	files           map[string]*fileData
	closed          bool
	outputChan      chan<- FileData
	openFiles       sync.WaitGroup // Track open files
	openFilesCount  atomic.Int64   // Tracks number of open files for monitoring
}

// FileData contains the filename and content of a file written to the FUSE mount
type FileData struct {
	Name   string
	Reader io.Reader
}

type fileData struct {
	content []byte
	mu      sync.Mutex
}

var fuseLogger = NewLogger("FUSE")

// NewFuseWatcher creates a new FUSE watcher that mounts at the specified path
// Backpressure is controlled by the capacity of outputChan
func NewFuseWatcher(mountPath string, outputChan chan<- FileData) (*FuseWatcher, error) {
	if err := os.MkdirAll(mountPath, 0755); err != nil {
		return nil, err
	}

	fuseLogger.Println("New FUSE watcher at", mountPath)

	fw := &FuseWatcher{
		mountPath:  mountPath,
		entries:    make(chan fs.DirEntry, 100),
		files:      make(map[string]*fileData),
		outputChan: outputChan,
	}

	fs := pathfs.NewPathNodeFs(&fuseFS{
		FileSystem: pathfs.NewDefaultFileSystem(),
		watcher:    fw,
	}, nil)
	server, _, err := nodefs.MountRoot(mountPath, fs.Root(), &nodefs.Options{
		AttrTimeout:  time.Second,
		EntryTimeout: time.Second,
	})
	if err != nil {
		return nil, err
	}

	fw.server = server

	fw.Start()

	return fw, nil
}

func NewTempDirFuseWatcher(outputChan chan<- FileData) (*FuseWatcher, error) {
	d, err := os.MkdirTemp("/tmp", "output-*")
	if err != nil {
		return nil, err
	}

	return NewFuseWatcher(d, outputChan)
}

// Entries returns a channel that receives directory entries as files are written
func (fw *FuseWatcher) Entries() <-chan fs.DirEntry {
	return fw.entries
}

// Start begins serving the FUSE filesystem
func (fw *FuseWatcher) Start() {
	fuseLogger.Printf("Starting server at %s\n", fw.mountPath)
	go fw.server.Serve()
}

// GetMountPath returns the mount path for this FUSE watcher
func (fw *FuseWatcher) GetMountPath() string {
	return fw.mountPath
}

// WaitForWrites blocks until all open files have been closed
func (fw *FuseWatcher) WaitForWrites() {
	fw.openFiles.Wait()
}

// Stop unmounts the filesystem, waits for all files to be released, closes channels, and cleans up the mount directory
func (fw *FuseWatcher) Stop() error {
	fw.mu.Lock()
	if fw.closed {
		fw.mu.Unlock()
		return nil
	}
	fw.closed = true
	fw.mu.Unlock()

	fuseLogger.Printf("Stopping server at %s\n", fw.mountPath)

	// Wait for any open files to be closed (with timeout)
	done := make(chan struct{})
	go func() {
		fw.openFiles.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All files closed normally
		fuseLogger.Printf("All files closed gracefully\n")
	case <-time.After(2 * time.Second):
		// Timeout - force process remaining files
		remaining := fw.openFilesCount.Load()
		if remaining > 0 {
			fuseLogger.Printf("Timeout waiting for %d open files, force processing\n", remaining)
			fw.forceProcessFiles()
		}
	}

	// Unmount the filesystem
	err := fw.server.Unmount()
	if err != nil {
		fuseLogger.Printf("Error unmounting: %v\n", err)
	}

	// Close channels
	close(fw.entries)

	// Clean up the mount directory
	if err := os.RemoveAll(fw.mountPath); err != nil {
		fuseLogger.Printf("Error removing mount directory %s: %v\n", fw.mountPath, err)
		return err
	}

	fuseLogger.Printf("Cleaned up mount directory %s\n", fw.mountPath)
	return nil
}

// forceProcessFiles processes any files still in the buffer
func (fw *FuseWatcher) forceProcessFiles() {
	fw.mu.Lock()
	// Copy the files map
	filesToProcess := make(map[string]*fileData)
	for name, data := range fw.files {
		filesToProcess[name] = data
	}
	fw.mu.Unlock()

	// Process each buffered file
	for name, data := range filesToProcess {
		data.mu.Lock()
		if len(data.content) > 0 {
			content := make([]byte, len(data.content))
			copy(content, data.content)
			data.mu.Unlock()

			// Send to output channel if available
			if fw.outputChan != nil {
				reader := &bytesReader{data: content}
				select {
				case fw.outputChan <- FileData{Name: name, Reader: reader}:
					fuseLogger.Printf("Force-processed file: %s\n", name)
				case <-time.After(1 * time.Second):
					fuseLogger.Printf("Timeout sending file %s to channel\n", name)
				}
			}
		} else {
			data.mu.Unlock()
		}
	}

	// Clear the files map
	fw.mu.Lock()
	fw.files = make(map[string]*fileData)
	fw.mu.Unlock()
}

// fuseFS implements the FUSE filesystem interface
type fuseFS struct {
	pathfs.FileSystem
	watcher *FuseWatcher
}

func (fs *fuseFS) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	if name == "" {
		// Root directory - write-only, no read/list permissions
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0200, // Write-only directory
		}, fuse.OK
	}

	fs.watcher.mu.Lock()
	_, exists := fs.watcher.files[name]
	fs.watcher.mu.Unlock()

	if exists {
		return &fuse.Attr{
			Mode: fuse.S_IFREG | 0200, // Write-only file
		}, fuse.OK
	}

	fuseLogger.Printf("getattr %s\n", name)

	return nil, fuse.ENOENT
}

func (fs *fuseFS) OpenDir(name string, context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	// Deny directory listing - write-only directory
	fuseLogger.Printf("opendir refused %s\n", name)
	return nil, fuse.EACCES
}

func (fs *fuseFS) Create(name string, flags uint32, mode uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	fs.watcher.mu.Lock()
	defer fs.watcher.mu.Unlock()

	if fs.watcher.closed {
		return nil, fuse.EROFS
	}

	fd := &fileData{content: make([]byte, 0)}
	fs.watcher.files[name] = fd
	fs.watcher.openFiles.Add(1) // Track this open file
	fs.watcher.openFilesCount.Add(1)

	fuseLogger.Printf("create %s flags=%d mode=%d\n", name, flags, mode)

	return &fuseFile{
		File:    nodefs.NewDefaultFile(),
		name:    name,
		data:    fd,
		watcher: fs.watcher,
	}, fuse.OK
}

func (fs *fuseFS) Unlink(name string, context *fuse.Context) fuse.Status {
	fs.watcher.mu.Lock()
	defer fs.watcher.mu.Unlock()

	fuseLogger.Printf("unlink %s\n", name)
	delete(fs.watcher.files, name)
	return fuse.OK
}

// fuseFile represents an open file in the FUSE filesystem
type fuseFile struct {
	nodefs.File
	name    string
	data    *fileData
	watcher *FuseWatcher
}

func (f *fuseFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	f.data.mu.Lock()
	defer f.data.mu.Unlock()

	// Extend buffer if needed
	newSize := int(off) + len(data)
	if newSize > len(f.data.content) {
		newContent := make([]byte, newSize)
		copy(newContent, f.data.content)
		f.data.content = newContent
	}

	// Only log first write to avoid spam for large files
	if off == 0 {
		fuseLogger.Printf("write %s started\n", f.name)
	}
	copy(f.data.content[off:], data)
	return uint32(len(data)), fuse.OK
}

func (f *fuseFile) Flush() fuse.Status {
	fuseLogger.Println("¯\\_(ツ)_/¯")
	return fuse.OK
}

func (f *fuseFile) Release() {
	// When file is closed, consume it
	f.data.mu.Lock()
	content := make([]byte, len(f.data.content))
	copy(content, f.data.content)
	f.data.mu.Unlock()

	if len(content) > 0 {
		f.watcher.mu.Lock()
		closed := f.watcher.closed
		f.watcher.mu.Unlock()

		if !closed {
			// Send file data to output channel - blocks until consumed
			if f.watcher.outputChan != nil {
				reader := &bytesReader{data: content}
				f.watcher.outputChan <- FileData{Name: f.name, Reader: reader}
			}
		}
	}

	fuseLogger.Printf("release %s\n", f.name)
	
	// DON'T delete from map - allow file to be opened/written again
	// Each Create() will replace the entry with fresh data
	
	// Signal that this file is closed
	f.watcher.openFilesCount.Add(-1)
	f.watcher.openFiles.Done()
}

// fuseDirEntry implements fs.DirEntry
type fuseDirEntry struct {
	name string
}

func (e *fuseDirEntry) Name() string {
	return e.name
}

func (e *fuseDirEntry) IsDir() bool {
	return false
}

func (e *fuseDirEntry) Type() fs.FileMode {
	return 0
}

func (e *fuseDirEntry) Info() (fs.FileInfo, error) {
	return &fuseFileInfo{name: e.name}, nil
}

// fuseFileInfo implements fs.FileInfo
type fuseFileInfo struct {
	name string
}

func (i *fuseFileInfo) Name() string       { return i.name }
func (i *fuseFileInfo) Size() int64        { return 0 }
func (i *fuseFileInfo) Mode() fs.FileMode  { return 0644 }
func (i *fuseFileInfo) ModTime() time.Time { return time.Now() }
func (i *fuseFileInfo) IsDir() bool        { return false }
func (i *fuseFileInfo) Sys() interface{}   { return nil }

// bytesReader wraps a byte slice to implement io.Reader
type bytesReader struct {
	data []byte
	pos  int
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
