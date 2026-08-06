package node

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	Namespace        = "msgrpc.node"
	defaultOutputMax = 200
)

var safeProgramName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

type RunnerOptions struct {
	Directory      string
	GoCommand      string
	Environment    map[string]string
	MaxOutputLines int
	BuildTimeout   time.Duration
	StopTimeout    time.Duration
}

type SaveOptions struct {
	GoMod string `json:"goMod,omitempty" msgpack:"goMod,omitempty"`
}

type ProgramInfo struct {
	Name     string        `json:"name" msgpack:"name"`
	Running  bool          `json:"running" msgpack:"running"`
	Built    bool          `json:"built" msgpack:"built"`
	PID      int           `json:"pid,omitempty" msgpack:"pid,omitempty"`
	Started  int64         `json:"startedAt,omitempty" msgpack:"startedAt,omitempty"`
	Ended    *ProgramEnded `json:"ended,omitempty" msgpack:"ended,omitempty"`
	ExitCode *int          `json:"exitCode,omitempty" msgpack:"exitCode,omitempty"`
	Language string        `json:"language" msgpack:"language"`
}

type ProgramEnded struct {
	Code   *int   `json:"code,omitempty" msgpack:"code,omitempty"`
	Signal string `json:"signal,omitempty" msgpack:"signal,omitempty"`
	At     int64  `json:"at" msgpack:"at"`
}

type ProgramFile struct {
	Name string `json:"name" msgpack:"name"`
	Path string `json:"path" msgpack:"path"`
}

type BuildResult struct {
	Name   string   `json:"name" msgpack:"name"`
	Binary string   `json:"binary" msgpack:"binary"`
	Output []string `json:"output,omitempty" msgpack:"output,omitempty"`
}

type StartResult struct {
	Name      string `json:"name" msgpack:"name"`
	PID       int    `json:"pid,omitempty" msgpack:"pid,omitempty"`
	StartedAt int64  `json:"startedAt" msgpack:"startedAt"`
}

type StopResult struct {
	Name  string        `json:"name" msgpack:"name"`
	Ended *ProgramEnded `json:"ended,omitempty" msgpack:"ended,omitempty"`
}

type OutputResult struct {
	Name    string        `json:"name" msgpack:"name"`
	Output  []string      `json:"output" msgpack:"output"`
	Running bool          `json:"running" msgpack:"running"`
	Ended   *ProgramEnded `json:"ended,omitempty" msgpack:"ended,omitempty"`
}

type RuntimeDescription struct {
	Directory string `json:"directory" msgpack:"directory"`
	GoCommand string `json:"goCommand" msgpack:"goCommand"`
	Namespace string `json:"namespace" msgpack:"namespace"`
}

type Runner struct {
	options RunnerOptions

	mu       sync.Mutex
	running  map[string]*processRecord
	finished map[string]*programRecord
}

type processRecord struct {
	cmd    *exec.Cmd
	record *programRecord
	done   chan struct{}
}

type programRecord struct {
	Name      string
	PID       int
	StartedAt int64
	Ended     *ProgramEnded
	Output    []string
}

func NewRunner(options RunnerOptions) *Runner {
	if options.Directory == "" {
		options.Directory = "programs"
	}
	if directory, err := filepath.Abs(options.Directory); err == nil {
		options.Directory = directory
	}
	if options.GoCommand == "" {
		options.GoCommand = "go"
	}
	if options.MaxOutputLines <= 0 {
		options.MaxOutputLines = defaultOutputMax
	}
	if options.BuildTimeout == 0 {
		options.BuildTimeout = 30 * time.Second
	}
	if options.StopTimeout == 0 {
		options.StopTimeout = 2 * time.Second
	}
	return &Runner{
		options:  options,
		running:  map[string]*processRecord{},
		finished: map[string]*programRecord{},
	}
}

func (r *Runner) Runtime() RuntimeDescription {
	return RuntimeDescription{
		Directory: r.options.Directory,
		GoCommand: r.options.GoCommand,
		Namespace: Namespace,
	}
}

func (r *Runner) Save(name, source string, options *SaveOptions) (ProgramFile, error) {
	dir, err := r.programDir(name)
	if err != nil {
		return ProgramFile{}, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return ProgramFile{}, err
	}
	mainPath := filepath.Join(dir, "main.go")
	if err := os.WriteFile(mainPath, []byte(ensureTrailingNewline(source)), 0o644); err != nil {
		return ProgramFile{}, err
	}
	goMod := defaultGoMod(name)
	if options != nil && options.GoMod != "" {
		goMod = options.GoMod
	}
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(ensureTrailingNewline(goMod)), 0o644); err != nil {
		return ProgramFile{}, err
	}
	return ProgramFile{Name: name, Path: mainPath}, nil
}

func (r *Runner) Read(name string) (string, error) {
	dir, err := r.programDir(name)
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(filepath.Join(dir, "main.go"))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (r *Runner) Delete(name string) (string, error) {
	if r.IsRunning(name) {
		if _, err := r.Stop(name); err != nil {
			return "", err
		}
	}
	dir, err := r.programDir(name)
	if err != nil {
		return "", err
	}
	if err := os.RemoveAll(dir); err != nil {
		return "", err
	}
	r.mu.Lock()
	delete(r.finished, name)
	r.mu.Unlock()
	return name, nil
}

func (r *Runner) List() ([]ProgramInfo, error) {
	if err := os.MkdirAll(r.options.Directory, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(r.options.Directory)
	if err != nil {
		return nil, err
	}
	infos := make([]ProgramInfo, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || !safeProgramName.MatchString(entry.Name()) {
			continue
		}
		info, err := r.Status(entry.Name())
		if err != nil {
			continue
		}
		infos = append(infos, info)
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos, nil
}

func (r *Runner) Build(ctx context.Context, name string) (BuildResult, error) {
	dir, err := r.existingProgramDir(name)
	if err != nil {
		return BuildResult{}, err
	}
	binary := r.binaryPath(name)
	if err := os.MkdirAll(filepath.Dir(binary), 0o755); err != nil {
		return BuildResult{}, err
	}
	buildCtx := ctx
	cancel := func() {}
	if _, ok := ctx.Deadline(); !ok && r.options.BuildTimeout > 0 {
		buildCtx, cancel = context.WithTimeout(ctx, r.options.BuildTimeout)
	}
	defer cancel()

	cmd := exec.CommandContext(buildCtx, r.options.GoCommand, "build", "-buildvcs=false", "-o", binary, ".")
	cmd.Dir = dir
	cmd.Env = r.commandEnv()
	output, err := cmd.CombinedOutput()
	lines := splitLines(output)
	if err != nil {
		message := fmt.Sprintf("build %s: %v", name, err)
		if len(lines) > 0 {
			message += "\n" + strings.Join(lines, "\n")
		}
		return BuildResult{Name: name, Binary: binary, Output: lines}, errors.New(message)
	}
	return BuildResult{Name: name, Binary: binary, Output: lines}, nil
}

func (r *Runner) Start(ctx context.Context, name string) (StartResult, error) {
	r.mu.Lock()
	if _, ok := r.running[name]; ok {
		r.mu.Unlock()
		return StartResult{}, fmt.Errorf("%q is already running", name)
	}
	r.mu.Unlock()

	stale, err := r.binaryStale(name)
	if err != nil {
		return StartResult{}, err
	}
	if stale {
		if _, err := r.Build(ctx, name); err != nil {
			return StartResult{}, err
		}
	}
	dir, err := r.existingProgramDir(name)
	if err != nil {
		return StartResult{}, err
	}
	cmd := exec.Command(r.binaryPath(name))
	cmd.Dir = dir
	cmd.Env = r.commandEnv()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return StartResult{}, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return StartResult{}, err
	}
	record := &programRecord{Name: name, StartedAt: time.Now().UnixMilli()}
	if err := cmd.Start(); err != nil {
		return StartResult{}, err
	}
	if cmd.Process != nil {
		record.PID = cmd.Process.Pid
	}

	r.mu.Lock()
	done := make(chan struct{})
	r.running[name] = &processRecord{cmd: cmd, record: record, done: done}
	delete(r.finished, name)
	r.mu.Unlock()

	go r.capture(record, "", stdout)
	go r.capture(record, "! ", stderr)
	go r.wait(name, cmd, record, done)

	return StartResult{Name: name, PID: record.PID, StartedAt: record.StartedAt}, nil
}

func (r *Runner) Stop(name string) (StopResult, error) {
	r.mu.Lock()
	entry := r.running[name]
	r.mu.Unlock()
	if entry == nil {
		return StopResult{}, fmt.Errorf("%q is not running", name)
	}
	if entry.cmd.Process != nil {
		_ = entry.cmd.Process.Signal(syscall.SIGTERM)
	}
	deadline := time.NewTimer(r.options.StopTimeout)
	defer deadline.Stop()
	select {
	case <-entry.done:
	case <-deadline.C:
		if entry.cmd.Process != nil {
			_ = entry.cmd.Process.Kill()
		}
		<-entry.done
	}
	return StopResult{Name: name, Ended: entry.record.Ended}, nil
}

func (r *Runner) Status(name string) (ProgramInfo, error) {
	if _, err := r.existingProgramDir(name); err != nil {
		return ProgramInfo{}, err
	}
	r.mu.Lock()
	if entry := r.running[name]; entry != nil {
		record := entry.record
		r.mu.Unlock()
		return ProgramInfo{
			Name:     name,
			Running:  true,
			Built:    fileExists(r.binaryPath(name)),
			PID:      record.PID,
			Started:  record.StartedAt,
			Language: "go",
		}, nil
	}
	record := r.finished[name]
	r.mu.Unlock()
	info := ProgramInfo{Name: name, Built: fileExists(r.binaryPath(name)), Language: "go"}
	if record != nil {
		info.Started = record.StartedAt
		info.Ended = record.Ended
		if record.Ended != nil {
			info.ExitCode = record.Ended.Code
		}
	}
	return info, nil
}

func (r *Runner) Output(name string) (OutputResult, error) {
	if _, err := r.existingProgramDir(name); err != nil {
		return OutputResult{}, err
	}
	r.mu.Lock()
	var record *programRecord
	running := false
	if entry := r.running[name]; entry != nil {
		record = entry.record
		running = true
	} else {
		record = r.finished[name]
	}
	if record == nil {
		r.mu.Unlock()
		return OutputResult{Name: name, Output: []string{}}, nil
	}
	output := append([]string(nil), record.Output...)
	ended := record.Ended
	r.mu.Unlock()
	return OutputResult{Name: name, Output: output, Running: running, Ended: ended}, nil
}

func (r *Runner) IsRunning(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.running[name] != nil
}

func (r *Runner) StopAll() {
	r.mu.Lock()
	names := make([]string, 0, len(r.running))
	for name := range r.running {
		names = append(names, name)
	}
	r.mu.Unlock()
	for _, name := range names {
		_, _ = r.Stop(name)
	}
}

func (r *Runner) capture(record *programRecord, prefix string, reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		r.mu.Lock()
		record.Output = append(record.Output, prefix+scanner.Text())
		if len(record.Output) > r.options.MaxOutputLines {
			record.Output = record.Output[len(record.Output)-r.options.MaxOutputLines:]
		}
		r.mu.Unlock()
	}
}

func (r *Runner) wait(name string, cmd *exec.Cmd, record *programRecord, done chan<- struct{}) {
	defer close(done)
	err := cmd.Wait()
	ended := ProgramEnded{At: time.Now().UnixMilli()}
	if cmd.ProcessState != nil {
		code := cmd.ProcessState.ExitCode()
		ended.Code = &code
		if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			ended.Signal = status.Signal().String()
		}
	} else if err != nil {
		code := -1
		ended.Code = &code
	}
	r.mu.Lock()
	record.Ended = &ended
	delete(r.running, name)
	r.finished[name] = record
	r.mu.Unlock()
}

func (r *Runner) existingProgramDir(name string) (string, error) {
	dir, err := r.programDir(name)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(filepath.Join(dir, "main.go")); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("no program named %q", name)
		}
		return "", err
	}
	return dir, nil
}

func (r *Runner) programDir(name string) (string, error) {
	if !safeProgramName.MatchString(name) {
		return "", fmt.Errorf("%q is not a usable program name", name)
	}
	root, err := filepath.Abs(r.options.Directory)
	if err != nil {
		return "", err
	}
	dir := filepath.Join(root, name)
	if filepath.Dir(dir) != root {
		return "", fmt.Errorf("%q would write outside the programs directory", name)
	}
	return dir, nil
}

func (r *Runner) binaryPath(name string) string {
	return filepath.Join(r.options.Directory, ".bin", name)
}

func (r *Runner) binaryStale(name string) (bool, error) {
	dir, err := r.existingProgramDir(name)
	if err != nil {
		return false, err
	}
	source, err := os.Stat(filepath.Join(dir, "main.go"))
	if err != nil {
		return false, err
	}
	binary, err := os.Stat(r.binaryPath(name))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		return false, err
	}
	return binary.ModTime().Before(source.ModTime()), nil
}

func (r *Runner) commandEnv() []string {
	env := os.Environ()
	for key, value := range r.options.Environment {
		env = append(env, key+"="+value)
	}
	if os.Getenv("GOCACHE") == "" {
		env = append(env, "GOCACHE="+filepath.Join(r.options.Directory, ".gocache"))
	}
	return env
}

func defaultGoMod(name string) string {
	return "module source-rpc-go-program/" + name + "\n\ngo 1.24.0\n"
}

func ensureTrailingNewline(value string) string {
	if value == "" || value[len(value)-1] != '\n' {
		return value + "\n"
	}
	return value
}

func splitLines(value []byte) []string {
	scanner := bufio.NewScanner(bytes.NewReader(value))
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
