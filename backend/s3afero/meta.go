package s3afero

import (
	"encoding/hex"
	"encoding/json"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/afero"
)

type Metadata struct {
	File    string
	ModTime time.Time
	Size    int64
	Hash    []byte
	Meta    map[string]string
}

type metaPath struct {
	bucket string
	object string
}

func (mp metaPath) FilePath() string {
	return filepath.Join(mp.bucket, mp.object)
}

type metaStore struct {
	fs          afero.Fs
	modTimeCalc modTimeCalc
	modTimeRes  time.Duration
}

func newMetaStore(fs afero.Fs, modTimeCalc modTimeCalc) *metaStore {
	b := &metaStore{
		fs:          fs,
		modTimeCalc: modTimeCalc,
		modTimeRes:  -1,
	}
	return b
}

func (ms *metaStore) getModTimeRes() (dur time.Duration, err error) {
	if ms.modTimeRes < 0 {
		ms.modTimeRes, err = ms.modTimeCalc()
		if err != nil {
			return -1, err
		}
	}
	return ms.modTimeRes, nil
}

func (ms *metaStore) metaPath(bucket string, object string) metaPath {
	// FIXME: may need to add path segments but that may be a thing of the past:
	// https://stackoverflow.com/questions/466521/how-many-files-can-i-put-in-a-directory
	h := fnv.New128a()
	h.Write([]byte(object))
	object = strings.Replace(object, "/", "_", -1)
	object = strings.Replace(object, "\\", "_", -1)

	return metaPath{bucket, object + "-" + hex.EncodeToString(h.Sum(nil))}
}

func (ms *metaStore) loadMeta(bucket string, object string, size int64, mtime time.Time) (*Metadata, error) {
	metaPath := ms.metaPath(bucket, object)
	fullPath := metaPath.FilePath()

	bts, err := afero.ReadFile(ms.fs, fullPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	} else if os.IsNotExist(err) {
                afero.WriteFile(ms.fs, fullPath, bts, 0666)
        }


	var meta Metadata
	if len(bts) > 0 {
		if err := json.Unmarshal(bts, &meta); err != nil {
			return nil, err
		}
	}

	modRes, err := ms.getModTimeRes()
	if err != nil {
		return nil, err
	}

	modDiff := mtime.Sub(meta.ModTime)
	if len(meta.Hash) == 0 || meta.Size != size || modDiff < -modRes || modDiff > modRes {
		meta.Size = size
		meta.ModTime = mtime
		meta.Hash, err = hashFile(ms.fs, fullPath)
		if err != nil {
			return nil, err
		}
		if meta.Meta == nil {
                        meta.Meta = map[string]string{"Content-Type":"text/x-go; charset=utf-8","Last-Modified":"Wed, 06 Apr 2022 03:00:22 GMT","X-Amz-Content-Sha256":"7849c6dff7210d71500f26ba4840813c30330a2ef46a0abdf1d145e289768628","X-Amz-Date":"20220406T030022Z","X-Amz-Storage-Class":"STANDARD"}
                }
                if meta.File == "" {
                        meta.File = bucket + "/" + object
                }

		if err := ms.saveMeta(metaPath, &meta); err != nil {
			return nil, err
		}
	}

	return &meta, nil
}

func (ms *metaStore) saveMeta(path metaPath, meta *Metadata) error {
	bts, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := ms.fs.MkdirAll(filepath.Dir(path.FilePath()), 0777); err != nil {
		return err
	}

	return afero.WriteFile(ms.fs, path.FilePath(), bts, 0666)
}

func (ms *metaStore) deleteMeta(path metaPath) error {
	if err := ms.fs.Remove(path.FilePath()); os.IsNotExist(err) {
		return nil
	} else {
		return err
	}
}

func (ms *metaStore) deleteBucket(bucket string) error {
	if err := ms.fs.RemoveAll(bucket); os.IsNotExist(err) {
		return nil
	} else {
		return err
	}
}
