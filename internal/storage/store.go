package storage

import (
	"archive/tar"
	"cicd-engine/internal/scheduler"
	"cicd-engine/internal/workflow"
	"compress/gzip"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Store struct{ Dir string }

// Coordinate snapshots and readers within this process. Cross-process readers
// additionally use share-delete on Windows; bounded retry handles AV/indexers.
var metadataMu sync.RWMutex

type Artifact struct {
	Name      string    `json:"name"`
	JobID     string    `json:"job_id"`
	Size      int64     `json:"size"`
	Files     int       `json:"files"`
	Checksum  string    `json:"sha256"`
	CreatedAt time.Time `json:"created_at"`
}

func New(dir string) (*Store, error) {
	p, e := filepath.Abs(dir)
	if e != nil {
		return nil, e
	}
	for _, s := range []string{"runs", "artifacts"} {
		if e = os.MkdirAll(filepath.Join(p, s), 0700); e != nil {
			return nil, e
		}
	}
	return &Store{p}, nil
}
func ID() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b[:])
}
func SafeJoin(base, rel string) (string, error) {
	if !workflow.SafeRelative(rel) {
		return "", fmt.Errorf("unsafe relative path %q", rel)
	}
	p := filepath.Join(base, rel)
	r, e := filepath.Rel(base, p)
	if e != nil || r == ".." || strings.HasPrefix(r, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes root")
	}
	return p, nil
}
func (s *Store) RunDir(id string) (string, error) {
	if !workflow.ValidID(id) {
		return "", fmt.Errorf("invalid run ID")
	}
	return SafeJoin(filepath.Join(s.Dir, "runs"), id)
}
func AtomicJSON(path string, v any) error {
	metadataMu.Lock()
	defer metadataMu.Unlock()
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".write-*")
	if e != nil {
		return e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	for attempt := 0; attempt < 10; attempt++ {
		e = os.Rename(tmp, path)
		if e == nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return e
}
func (s *Store) Save(r *scheduler.Run) error {
	p, e := s.RunDir(r.ID)
	if e != nil {
		return e
	}
	if e = os.MkdirAll(p, 0700); e != nil {
		return e
	}
	return AtomicJSON(filepath.Join(p, "run.json"), r)
}
func (s *Store) Load(id string) (*scheduler.Run, error) {
	p, e := s.RunDir(id)
	if e != nil {
		return nil, e
	}
	metadataMu.RLock()
	b, e := readMetadata(filepath.Join(p, "run.json"))
	metadataMu.RUnlock()
	if e != nil {
		return nil, e
	}
	var r scheduler.Run
	e = json.Unmarshal(b, &r)
	if e == nil && r.SchemaVersion != 1 {
		e = fmt.Errorf("unsupported metadata schema %d", r.SchemaVersion)
	}
	return &r, e
}
func (s *Store) List() ([]*scheduler.Run, error) {
	entries, e := os.ReadDir(filepath.Join(s.Dir, "runs"))
	if e != nil {
		return nil, e
	}
	out := []*scheduler.Run{}
	for _, d := range entries {
		if d.IsDir() {
			r, e := s.Load(d.Name())
			if os.IsNotExist(e) {
				continue
			} // An accepted run may be creating its initial snapshot.
			if e != nil {
				return nil, e
			}
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}
func (s *Store) Recover() error {
	rs, e := s.List()
	if e != nil {
		return e
	}
	for _, r := range rs {
		if !r.State.Terminal() {
			r.State = scheduler.Interrupted
			r.FinishedAt = time.Now().UTC()
			for id, j := range r.Jobs {
				if !j.State.Terminal() {
					j.State = scheduler.Interrupted
					j.FinishedAt = r.FinishedAt
					for i := range j.Steps {
						if !j.Steps[i].State.Terminal() {
							j.Steps[i].State = scheduler.Interrupted
							j.Steps[i].FinishedAt = r.FinishedAt
						}
					}
					r.Jobs[id] = j
				}
			}
			if e = s.Save(r); e != nil {
				return e
			}
		}
	}
	return nil
}

// CopySnapshot rejects all symlinks and uses os.Root to enforce traversal boundaries.
func CopySnapshot(source, dest, exclude string) error {
	source, e := filepath.Abs(source)
	if e != nil {
		return e
	}
	dest, e = filepath.Abs(dest)
	if e != nil {
		return e
	}
	exclude, _ = filepath.Abs(exclude)
	if e = os.MkdirAll(dest, 0700); e != nil {
		return e
	}
	src, e := os.OpenRoot(source)
	if e != nil {
		return e
	}
	defer src.Close()
	dst, e := os.OpenRoot(dest)
	if e != nil {
		return e
	}
	defer dst.Close()
	var total int64
	count := 0
	return fs.WalkDir(src.FS(), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == "." {
			return nil
		}
		host := filepath.Join(source, filepath.FromSlash(path))
		name := d.Name()
		if d.IsDir() && (name == ".git" || name == ".cicd-data" || host == dest || host == exclude) {
			return fs.SkipDir
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("snapshot symlink rejected: %s", path)
		}
		if d.IsDir() {
			return dst.MkdirAll(filepath.FromSlash(path), 0700)
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("nonregular snapshot file %s", path)
		}
		total += info.Size()
		count++
		if total > 1<<30 || count > 100000 {
			return fmt.Errorf("snapshot exceeds 1GiB/100000 file limit")
		}
		in, e := src.Open(filepath.FromSlash(path))
		if e != nil {
			return e
		}
		defer in.Close()
		out, e := dst.OpenFile(filepath.FromSlash(path), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode().Perm()&0777)
		if e != nil {
			return e
		}
		_, e = io.Copy(out, in)
		ce := out.Close()
		if e != nil {
			return e
		}
		return ce
	})
}
func (s *Store) ArtifactPath(id, name string) (string, error) {
	if !workflow.ValidID(id) || !workflow.ValidID(name) {
		return "", fmt.Errorf("invalid artifact identifier")
	}
	return filepath.Join(s.Dir, "artifacts", id, name+".tar.gz"), nil
}
func (s *Store) Artifacts(id string) ([]Artifact, error) {
	if !workflow.ValidID(id) {
		return nil, fmt.Errorf("invalid run ID")
	}
	dir := filepath.Join(s.Dir, "artifacts", id)
	es, e := os.ReadDir(dir)
	if os.IsNotExist(e) {
		return []Artifact{}, nil
	}
	if e != nil {
		return nil, e
	}
	out := []Artifact{}
	for _, v := range es {
		if strings.HasSuffix(v.Name(), ".json") {
			b, e := os.ReadFile(filepath.Join(dir, v.Name()))
			if e != nil {
				return nil, e
			}
			var a Artifact
			if e = json.Unmarshal(b, &a); e != nil {
				return nil, e
			}
			out = append(out, a)
		}
	}
	return out, nil
}
func (s *Store) Upload(id, job, workspace string, a workflow.ArtifactConfig, maxBytes int64) (Artifact, error) {
	meta := Artifact{Name: a.Name, JobID: job, CreatedAt: time.Now().UTC()}
	if !workflow.SafeRelative(a.Path) {
		return meta, fmt.Errorf("unsafe artifact path")
	}
	path, e := s.ArtifactPath(id, a.Name)
	if e != nil {
		return meta, e
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return meta, e
	}
	root, e := os.OpenRoot(workspace)
	if e != nil {
		return meta, e
	}
	defer root.Close()
	// Reject symlinks in every prefix, not only the final file.
	part := ""
	for _, p := range strings.Split(filepath.ToSlash(a.Path), "/") {
		part = filepath.Join(part, p)
		i, e := root.Lstat(part)
		if e != nil {
			return meta, e
		}
		if i.Mode()&os.ModeSymlink != 0 {
			return meta, fmt.Errorf("artifact symlink rejected")
		}
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".artifact-*")
	if e != nil {
		return meta, e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	h := sha256.New()
	gz := gzip.NewWriter(io.MultiWriter(f, h))
	tw := tar.NewWriter(gz)
	var raw int64
	e = fs.WalkDir(root.FS(), filepath.ToSlash(a.Path), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		i, e := d.Info()
		if e != nil {
			return e
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("artifact symlink rejected: %s", p)
		}
		if !i.IsDir() && !i.Mode().IsRegular() {
			return fmt.Errorf("nonregular artifact")
		}
		if i.IsDir() {
			return nil
		}
		meta.Files++
		raw += i.Size()
		if raw > maxBytes || meta.Files > 10000 {
			return fmt.Errorf("artifact exceeds size/file limit")
		}
		hdr, e := tar.FileInfoHeader(i, "")
		if e != nil {
			return e
		}
		hdr.Name = p
		hdr.Mode = int64(i.Mode().Perm() & 0777)
		hdr.Uid = 0
		hdr.Gid = 0
		hdr.Uname = ""
		hdr.Gname = ""
		if e = tw.WriteHeader(hdr); e != nil {
			return e
		}
		in, e := root.Open(filepath.FromSlash(p))
		if e != nil {
			return e
		}
		defer in.Close()
		_, e = io.CopyN(tw, in, i.Size())
		return e
	})
	e = errors.Join(e, tw.Close(), gz.Close())
	if e != nil {
		return meta, e
	}
	if meta.Files == 0 {
		return meta, fmt.Errorf("empty artifact")
	}
	if e = f.Sync(); e != nil {
		return meta, e
	}
	info, e := f.Stat()
	if e != nil {
		return meta, e
	}
	meta.Size = info.Size()
	meta.Checksum = hex.EncodeToString(h.Sum(nil))
	if e = f.Close(); e != nil {
		return meta, e
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return meta, e
	}
	verify, e := os.Open(path)
	if e != nil {
		return meta, e
	}
	check := sha256.New()
	_, e = io.Copy(check, verify)
	verify.Close()
	if e != nil {
		return meta, e
	}
	if hex.EncodeToString(check.Sum(nil)) != meta.Checksum {
		return meta, fmt.Errorf("artifact checksum mismatch")
	}
	return meta, AtomicJSON(strings.TrimSuffix(path, ".tar.gz")+".json", meta)
}

// Retention only removes artifacts for terminal runs. Active uploads are never touched.
func (s *Store) Prune(age time.Duration) error {
	rs, e := s.List()
	if e != nil {
		return e
	}
	for _, r := range rs {
		if r.State.Terminal() && !r.FinishedAt.IsZero() && time.Since(r.FinishedAt) > age {
			if e = os.RemoveAll(filepath.Join(s.Dir, "artifacts", r.ID)); e != nil {
				return e
			}
		}
	}
	return nil
}
