package main

import (
	"bytes"
	"io"
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

// Start begins serving the FUSE filesystem
func (fw *FuseWatcher) Start() {
	fuseLogger.Verbosef("Starting server at %s\n", fw.mountPath)
	go fw.server.Serve()
}

// GetMountPath returns the mount path for this FUSE watcher
func (fw *FuseWatcher) GetMountPath() string {
	return fw.mountPath
}

// WaitForWrites blocks until all open files have been closed
func (fw *FuseWatcher) WaitForWrites() {
	if fw == nil {
		panic("how is this a nil")
	}
	fw.openFiles.Wait()
}

// Stop unmounts the filesystem, waits for open files to be released, and cleans up the mount directory
func (fw *FuseWatcher) Stop() error {
	fw.mu.Lock()
	if fw.closed {
		fw.mu.Unlock()
		return nil
	}
	fw.closed = true
	fw.mu.Unlock()

	fuseLogger.Verbosef("Stopping server at %s\n", fw.mountPath)

	// Wait for any open files to be closed (with short timeout)
	done := make(chan struct{})
	go func() {
		fw.openFiles.Wait()
		close(done)
	}()

	select {
	case <-done:
		fuseLogger.Verbosef("All files closed gracefully\n")
	case <-time.After(2 * time.Second):
		fuseLogger.Verbosef("Timeout waiting for open files, continuing shutdown\n")
	}

	// Unmount the filesystem
	err := fw.server.Unmount()
	if err != nil {
		fuseLogger.Verbosef("Error unmounting: %v\n", err)
	}

	// Clean up the mount directory
	if err := os.RemoveAll(fw.mountPath); err != nil {
		fuseLogger.Verbosef("Error removing mount directory %s: %v\n", fw.mountPath, err)
		return err
	}

	fuseLogger.Verbosef("Cleaned up mount directory %s\n", fw.mountPath)

	return nil
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

	fuseLogger.Verbosef("getattr %s\n", name)

	return nil, fuse.ENOENT
}

func (fs *fuseFS) OpenDir(name string, context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	// Deny directory listing - write-only directory
	fuseLogger.Verbosef("opendir refused %s\n", name)
	return nil, fuse.EACCES
}

func (fs *fuseFS) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	fs.watcher.mu.Lock()
	defer fs.watcher.mu.Unlock()

	if fs.watcher.closed {
		return nil, fuse.EROFS
	}

	// Check if opening for read - deny read access (write-only filesystem)
	accessMode := flags & 0x3 // O_RDONLY=0, O_WRONLY=1, O_RDWR=2
	if accessMode == 0 {      // O_RDONLY
		fuseLogger.Verbosef("open %s denied - read access not permitted\n", name)
		return nil, fuse.EACCES
	}

	// For write-only filesystem: allow opening any file for write
	// Each open creates fresh content (like O_TRUNC behavior)
	fd := &fileData{content: make([]byte, 0)}
	fs.watcher.files[name] = fd
	fs.watcher.openFiles.Add(1) // Track this open file

	fuseLogger.Verbosef("open %s flags=0x%x (write)\n", name, flags)

	return &fuseFile{
		File:    nodefs.NewDefaultFile(),
		name:    name,
		data:    fd,
		watcher: fs.watcher,
	}, fuse.OK
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

	fuseLogger.Verbosef("create %s flags=%d mode=%d\n", name, flags, mode)

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

	fuseLogger.Verbosef("unlink %s\n", name)
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
		fuseLogger.Verbosef("write %s started\n", f.name)
	}
	copy(f.data.content[off:], data)
	return uint32(len(data)), fuse.OK
}

func (f *fuseFile) Flush() fuse.Status {
	return fuse.OK
}

func (f *fuseFile) Fsync(flags int) fuse.Status {
	return fuse.OK
}

func (f *fuseFile) Truncate(size uint64) fuse.Status {
	f.data.mu.Lock()
	defer f.data.mu.Unlock()

	if uint64(len(f.data.content)) != size {
		newContent := make([]byte, size)
		copy(newContent, f.data.content)
		f.data.content = newContent
	}
	return fuse.OK
}

func (f *fuseFile) GetAttr(out *fuse.Attr) fuse.Status {
	f.data.mu.Lock()
	defer f.data.mu.Unlock()

	out.Mode = fuse.S_IFREG | 0200 // Write-only file
	out.Size = uint64(len(f.data.content))
	return fuse.OK
}

func (f *fuseFile) Allocate(off uint64, size uint64, mode uint32) fuse.Status {
	// Pre-allocate space if needed
	f.data.mu.Lock()
	defer f.data.mu.Unlock()

	requiredSize := int(off + size)
	if requiredSize > len(f.data.content) {
		newContent := make([]byte, requiredSize)
		copy(newContent, f.data.content)
		f.data.content = newContent
	}
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
				reader := bytes.NewReader(content)
				f.watcher.outputChan <- FileData{Name: f.name, Reader: reader}
			}
		}
	}

	fuseLogger.Verbosef("release %s\n", f.name)

	// DON'T delete from map - allow file to be opened/written again
	// Each Create() will replace the entry with fresh data

	// Signal that this file is closed
	f.watcher.openFiles.Done()
}
