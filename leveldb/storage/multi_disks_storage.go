// Copyright@2018, Baidu,Inc.
// sunjunyi01@baidu.com

package storage

import (
	"path/filepath"
	"os"
	"strings"
	"fmt"
)

func (fs *fileStorage) getRealPath(name string) string {
	var fdNum uint64
	fmt.Sscanf(filepath.Base(name), "%d.ldb", &fdNum)
	N := uint64(len(fs.dataPaths))
	return filepath.Join(fs.dataPaths[fdNum%N], fmt.Sprintf(SST_FORMAT, fdNum))
}

func (fs *fileStorage) MultiGuessOpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	var fdNum uint64
	var openErr error
	var fileHandler *os.File
	fmt.Sscanf(filepath.Base(name), "%d.ldb", &fdNum)
	for _, dp := range fs.dataPaths {
		fullName := filepath.Join(dp, fmt.Sprintf(SST_FORMAT, fdNum))
		fileHandler, openErr = os.OpenFile(fullName, flag, perm)
		if openErr == nil {
			return fileHandler, nil
		}
	}
	return nil, openErr
}

func (fs *fileStorage) MultiGuessRemove(name string) error {
	var fdNum uint64
	var removeErr error
	fmt.Sscanf(filepath.Base(name), "%d.ldb", &fdNum)
	for _, dp := range fs.dataPaths {
		fullName := filepath.Join(dp, fmt.Sprintf(SST_FORMAT, fdNum))
		removeErr = os.Remove(fullName)
		if removeErr == nil {
			return nil
		}
	}
	return removeErr
}

func (fs *fileStorage) MultiOpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	if len(fs.dataPaths) > 0 && strings.HasSuffix(name, ".ldb") {
		realName := fs.getRealPath(name)
		fileHandler, openErr := os.OpenFile(realName, flag, perm)
		if openErr == nil {
			return fileHandler, nil
		}
		switch openErr.(type) {
		case *os.PathError:
			if flag == os.O_RDONLY { //ready only
				return fs.MultiGuessOpenFile(name, flag, perm)
			} else {
				return nil, openErr
			}
		default:
			return nil, openErr
		}
	} else {
		return os.OpenFile(name, flag, perm)
	}
}

func (fs *fileStorage) MultiRemove(name string) error {
	if len(fs.dataPaths) > 0 && strings.HasSuffix(name, ".ldb") {
		realName := fs.getRealPath(name)
		removeErr := os.Remove(realName)
		if removeErr != nil {
			return fs.MultiGuessRemove(name)
		}
		return nil
	} else {
		return os.Remove(name)
	}
}

func (fs *fileStorage) MultiStat(name string) (os.FileInfo, error) {
	if len(fs.dataPaths) > 0 && strings.HasSuffix(name, ".ldb") {
		realName := fs.getRealPath(name)
		return os.Stat(realName)
	} else {
		return os.Stat(name)
	}
}

func (fs *fileStorage) MultiList(ft FileType) (fds []FileDesc, err error) {
	for _, path := range fs.dataPaths {
		dir, err := os.Open(path)
		if err != nil {
			return fds, err
		}
		names, err := dir.Readdirnames(0)
		// Close the dir first before checking for Readdirnames error.
		if cerr := dir.Close(); cerr != nil {
			fs.log(fmt.Sprintf("close dir: %v", cerr))
		}
		if err == nil {
			for _, name := range names {
				if fd, ok := fsParseName(name); ok && fd.Type&ft != 0 {
					fds = append(fds, fd)
				}
			}
		}
	}
	return
}

