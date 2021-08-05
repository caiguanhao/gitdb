package gitdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/ssh"
	xssh "golang.org/x/crypto/ssh"
)

type (
	DB struct {
		Remote string
		Local  string

		RemoteName string
		BranchName string

		UserName  string
		UserEmail string

		publicKey *ssh.PublicKeys
	}

	Collection struct {
		db *DB

		Path string

		JSONPCallbackName string
	}

	Object struct {
		db *DB

		Path string

		JSONPCallbackName string
	}

	Marshaler interface {
		GITDBMarshalJSON() []byte
	}
)

func NewDB(remote, local string) *DB {
	return &DB{
		Remote: remote,
		Local:  local,
	}
}

func (db *DB) SetSSHKey(user string, pemBytes []byte, password string) error {
	publicKey, err := ssh.NewPublicKeys(user, pemBytes, password)
	if err == nil {
		publicKey.HostKeyCallback = xssh.InsecureIgnoreHostKey()
		db.publicKey = publicKey
	}
	return err
}

func (db *DB) SetUser(name, email string) {
	db.UserName = name
	db.UserEmail = email
}

func (db DB) GetRemoteName() string {
	remote := db.RemoteName
	if remote == "" {
		return "origin"
	}
	return remote
}

func (db *DB) SetRemoteName(name string) {
	db.RemoteName = name
}

func (db DB) GetBranchName() string {
	branch := db.BranchName
	if branch == "" {
		return "master"
	}
	return branch
}

func (db *DB) SetBranchName(name string) {
	db.BranchName = name
}

func (db DB) MustInit() {
	if err := db.Init(); err != nil {
		panic(err)
	}
}

func (db DB) Init() error {
	log.Println("initializing", db.Remote)
	r, err := git.PlainClone(db.Local, false, &git.CloneOptions{
		URL:  db.Remote,
		Auth: db.publicKey,
	})
	if err == transport.ErrEmptyRemoteRepository {
		log.Println("init", db.Local)
		r, err = git.PlainInit(db.Local, false)
		if err == nil {
			_, err = r.CreateRemote(&config.RemoteConfig{
				Name: db.Remote,
				URLs: []string{db.Remote},
			})
		}
	}
	if err == git.ErrRepositoryAlreadyExists {
		_, err = git.PlainOpen(db.Local)
	}
	return err
}

func (db DB) MustForceUpdate() {
	if err := db.ForceUpdate(); err != nil {
		panic(err)
	}
}

func (db DB) ForceUpdate() error {
	r, err := git.PlainOpen(db.Local)
	if err != nil {
		return err
	}
	w, err := r.Worktree()
	if err != nil {
		return err
	}
	log.Println("fetching", db.GetRemoteName())
	err = r.Fetch(&git.FetchOptions{
		RemoteName: db.GetRemoteName(),
		Auth:       db.publicKey,
		Force:      true,
	})
	if err == transport.ErrEmptyRemoteRepository {
		return nil
	}
	if err != nil && err != git.NoErrAlreadyUpToDate {
		return err
	}
	ref, e := r.Reference(plumbing.NewRemoteReferenceName(db.GetRemoteName(), db.GetBranchName()), true)
	if e != nil {
		return e
	}
	err = w.Checkout(&git.CheckoutOptions{
		Branch: plumbing.NewBranchReferenceName(db.GetBranchName()),
		Force:  true,
	})
	if err != nil {
		return err
	}
	err = w.Reset(&git.ResetOptions{
		Mode:   git.HardReset,
		Commit: ref.Hash(),
	})
	return err
}

func (db *DB) NewCollection(path string) *Collection {
	return &Collection{
		db:   db,
		Path: path,
	}
}

func (db *DB) NewObject(path string) *Object {
	return &Object{
		db:   db,
		Path: path,
	}
}

func (db DB) MustAdd(message ...string) {
	if err := db.Add(message...); err != nil {
		panic(err)
	}
}

func (db DB) Add(files ...string) error {
	r, err := git.PlainOpen(db.Local)
	if err != nil {
		return err
	}
	w, err := r.Worktree()
	if err != nil {
		return err
	}
	for _, file := range files {
		if _, err := w.Add(file); err != nil {
			return err
		}
	}
	return nil
}

func (db DB) MustCommit(message ...string) {
	if err := db.Commit(message...); err != nil {
		panic(err)
	}
}

func (db DB) Commit(message ...string) error {
	r, err := git.PlainOpen(db.Local)
	if err != nil {
		return err
	}
	w, err := r.Worktree()
	if err != nil {
		return err
	}
	s, err := w.Status()
	if err != nil {
		return err
	}
	if s.IsClean() {
		log.Println("nothing to commit")
		return nil
	}
	var msg string
	if len(message) > 0 {
		msg = message[0]
	} else {
		msg = "update"
	}
	hash, err := w.Commit(msg, &git.CommitOptions{
		Author: &object.Signature{
			Name:  db.UserName,
			Email: db.UserEmail,
			When:  time.Now(),
		},
	})
	if err == nil {
		log.Println("added commit", hash.String()[:8])
	} else {
		log.Println("error adding commit", err)
	}
	return err
}

func (db DB) MustUnpushedCommits() []string {
	commits, err := db.UnpushedCommits()
	if err != nil {
		panic(err)
	}
	return commits
}

func (db DB) UnpushedCommits() ([]string, error) {
	r, err := git.PlainOpen(db.Local)
	if err != nil {
		return nil, err
	}
	head, err := r.Head()
	if err != nil {
		return nil, err
	}
	ref, err := r.Reference(plumbing.NewRemoteReferenceName(db.GetRemoteName(), db.GetBranchName()), true)
	if err != nil {
		return nil, err
	}
	c, err := object.GetCommit(r.Storer, head.Hash())
	if err != nil {
		return nil, err
	}
	refHash := ref.Hash()
	var commits []string
	iter := object.NewCommitPreorderIter(c, nil, nil)
	iter.ForEach(func(c *object.Commit) error {
		if c.Hash == refHash {
			return storer.ErrStop
		}
		commits = append(commits, c.Hash.String())
		return nil
	})
	return commits, nil
}

func (db DB) MustPush() {
	if err := db.Push(); err != nil {
		panic(err)
	}
}

func (db DB) Push() error {
	r, err := git.PlainOpen(db.Local)
	if err != nil {
		return err
	}
	return r.Push(&git.PushOptions{
		Auth: db.publicKey,
	})
}

func (c Collection) MustRead(dest interface{}) {
	if err := c.Read(dest); err != nil {
		panic(err)
	}
}

func (c Collection) Read(dest interface{}) error {
	defer removeNulls(dest)
	path := filepath.Join(c.db.Local, c.Path)
	return readJson(path, dest)
}

func (c Collection) MustWrite(content interface{}, funcs ...interface{}) {
	if err := c.Write(content, funcs...); err != nil {
		panic(err)
	}
}

func (c Collection) Write(content interface{}, funcs ...interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Write: %w", r)
		}
	}()
	w := write(c.JSONPCallbackName, content, funcs...)
	path := filepath.Join(c.db.Local, c.Path)
	os.MkdirAll(filepath.Dir(path), 0755)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, w)
	return err
}

func (o Object) MustDelete() {
	if err := o.Delete(); err != nil {
		panic(err)
	}
}

func (o Object) Delete() error {
	path := filepath.Join(o.db.Local, o.Path)
	return os.Remove(path)
}

func (o Object) MustRead(dest interface{}) {
	if err := o.Read(dest); err != nil {
		panic(err)
	}
}

func (o Object) Read(dest interface{}) error {
	path := filepath.Join(o.db.Local, o.Path)
	return readJson(path, dest)
}

func (o Object) MustWrite(content interface{}) {
	if err := o.Write(content); err != nil {
		panic(err)
	}
}

func (o Object) Write(content interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Write: %w", r)
		}
	}()
	w := write(o.JSONPCallbackName, content)
	path := filepath.Join(o.db.Local, o.Path)
	os.MkdirAll(filepath.Dir(path), 0755)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, w)
	return err
}

func readJson(path string, dest interface{}) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var start int64
	buf := make([]byte, 100)
	f.Read(buf)
	a := bytes.IndexAny(buf, "[{")
	x := bytes.IndexByte(buf, '(')
	if x > -1 && x < a {
		start = int64(x) + 1
	}

	n, _ := f.Seek(-100, 2)
	f.Read(buf)
	b := bytes.IndexAny(buf, "}]")
	y := bytes.LastIndexByte(buf, ')')

	f.Seek(start, 0)
	if y > -1 && y > b {
		return json.NewDecoder(&io.LimitedReader{R: f, N: n + int64(y) - start}).Decode(dest)
	}
	return json.NewDecoder(f).Decode(dest)
}

func write(jsonpName string, content interface{}, funcs ...interface{}) io.Reader {
	w := &bytes.Buffer{}
	if jsonpName != "" {
		fmt.Fprintln(w, "// Generated by gitdb. DO NOT EDIT.")
		fmt.Fprintln(w, jsonpName+"(")
	}
	rv := reflect.ValueOf(content)
	kind := rv.Kind()
	if kind == reflect.Slice || kind == reflect.Array {
		fmt.Fprintln(w, "[")
	outer:
		for i := 0; i < rv.Len(); i++ {
			item := rv.Index(i)
			for j := 0; j < len(funcs); j++ {
				frv := reflect.ValueOf(funcs[j])
				ret := frv.Call([]reflect.Value{item.Addr()})
				if ret[0].IsNil() {
					continue outer
				}
				item = ret[0].Elem()
			}
			elem := item.Interface()
			if p, ok := elem.(Marshaler); ok {
				fmt.Fprint(w, string(p.GITDBMarshalJSON()), ",")
			} else {
				j, _ := json.Marshal(elem)
				fmt.Fprint(w, string(j), ",")
			}
			fmt.Fprintln(w)
		}
		fmt.Fprintln(w, "null")
		fmt.Fprint(w, "]")
	} else if kind == reflect.Struct {
		elem := rv.Interface()
		if p, ok := elem.(Marshaler); ok {
			fmt.Fprint(w, string(p.GITDBMarshalJSON()))
		} else {
			j, _ := json.Marshal(elem)
			fmt.Fprint(w, string(j))
		}
	}
	fmt.Fprintln(w)
	if jsonpName != "" {
		fmt.Fprintln(w, ")")
	}
	return w
}

func removeNulls(dest interface{}) {
	rv := reflect.Indirect(reflect.ValueOf(dest))
	for i := 0; i < rv.Len(); i++ {
		elem := rv.Index(i)
		if elem.IsZero() {
			rv.Set(reflect.AppendSlice(rv.Slice(0, i), rv.Slice(i+1, rv.Len())))
		}
	}
}
