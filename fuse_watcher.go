package main

import (
	"io"
	"io/fs"
	"log"
	"os"
	"sync"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
)

// FuseWatcher watches a FUSE mount point and consumes files written to it
type FuseWatcher struct {
	mountPath  string
	server     *fuse.Server
	entries    chan fs.DirEntry
	mu         sync.Mutex
	files      map[string]*fileData
	closed     bool
	outputChan chan<- FileData
	openFiles  sync.WaitGroup // Track open files
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

var fuseLogger = log.New(os.Stderr, "[FUSE]", LOG_FLAGS)

// NewFuseWatcher creates a new FUSE watcher that mounts at the specified path
// Backpressure is controlled by the capacity of outputChan
func NewFuseWatcher(mountPath string, outputChan chan<- FileData) (*FuseWatcher, error) {
	if err := os.MkdirAll(mountPath, 0755); err != nil {
		return nil, err
	}

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
	fuseLogger.Println("Starting server") 
	go fw.server.Serve()
}

// WaitForWrites blocks until all open files have been closed
func (fw *FuseWatcher) WaitForWrites() {
	fw.openFiles.Wait()
}

// Stop unmounts the filesystem, waits for all files to be released, and closes the entries channel
func (fw *FuseWatcher) Stop() error {
	fw.mu.Lock()
	if fw.closed {
		fw.mu.Unlock()
		return nil
	}
	fw.closed = true
	fw.mu.Unlock()

	// Wait for all open files to be released
	fuseLogger.Println("Waiting for all open files to be released...")
	fw.openFiles.Wait()
	fuseLogger.Println("All files released")

	err := fw.server.Unmount()
	close(fw.entries)
	fuseLogger.Println("Stopping server") 
	return err
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

	fuseLogger.Printf("write %s %d\n", f.name, len(data))
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
		// Send directory entry through channel
		entry := &fuseDirEntry{name: f.name}

		f.watcher.mu.Lock()
		closed := f.watcher.closed
		f.watcher.mu.Unlock()

		if !closed {
			// Block until we can send (provides backpressure)
			f.watcher.entries <- entry

			// Send file data to output channel - blocks until consumed
			if f.watcher.outputChan != nil {
				reader := &bytesReader{data: content}
				f.watcher.outputChan <- FileData{Name: f.name, Reader: reader}
			}
		}
	}

	fuseLogger.Printf("release %s %d\n", f.name)
	// Clean up file from map
	f.watcher.mu.Lock()
	delete(f.watcher.files, f.name)
	f.watcher.mu.Unlock()
	
	// Signal that this file is closed
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
