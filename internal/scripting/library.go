package scripting

import (
	"bufio"
	"errors"
	"regexp"
	"strings"
	"sync"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"

	lua "github.com/yuin/gopher-lua"
)

var shebangRe = regexp.MustCompile(`^#!\s*(\w+)\s+name\s*=\s*(\S+)`)

type LibraryDef struct {
	Name   string
	Engine string
	Code   string
	Funcs  []string
}

type LibraryManager struct {
	mu        sync.RWMutex
	libs      map[string]*LibraryDef
	funcToLib map[string]string
	running   bool
}

var libMgr = &LibraryManager{
	libs:      make(map[string]*LibraryDef),
	funcToLib: make(map[string]string),
}

func Libraries() *LibraryManager {
	return libMgr
}

func ParseShebang(code string) (engine, name string, ok bool) {
	scanner := bufio.NewScanner(strings.NewReader(code))
	if !scanner.Scan() {
		return "", "", false
	}
	line := strings.TrimSpace(scanner.Text())
	if !strings.HasPrefix(line, "#!") {
		return "", "", false
	}
	matches := shebangRe.FindStringSubmatch(line)
	if matches == nil {
		return "", "", false
	}
	return strings.ToLower(matches[1]), matches[2], true
}

func (m *LibraryManager) Load(code string, replace bool) (string, error) {
	engine, name, ok := ParseShebang(code)
	if !ok {
		return "", errors.New("ERR invalid shebang")
	}
	if engine != "lua" {
		return "", errors.New("ERR unsupported engine")
	}

	funcNames, err := CompileLibraryCode(code)
	if err != nil {
		return "", err
	}
	if len(funcNames) == 0 {
		return "", errors.New("ERR no functions registered")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.libs[name]; exists && !replace {
		return "", errors.New("ERR library already exists")
	}

	if old, exists := m.libs[name]; exists {
		for _, fn := range old.Funcs {
			delete(m.funcToLib, fn)
		}
	}

	m.libs[name] = &LibraryDef{
		Name:   name,
		Engine: engine,
		Code:   code,
		Funcs:  funcNames,
	}
	for _, fn := range funcNames {
		m.funcToLib[fn] = name
	}
	return name, nil
}

func (m *LibraryManager) Get(name string) (*LibraryDef, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	lib, ok := m.libs[name]
	return lib, ok
}

func (m *LibraryManager) FindLibrary(funcName string) (*LibraryDef, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	libName, ok := m.funcToLib[funcName]
	if ok {
		return m.libs[libName], true
	}
	return nil, false
}

func (m *LibraryManager) CacheFuncMapping(libName, funcName string) {
	m.mu.Lock()
	m.funcToLib[funcName] = libName
	if lib, ok := m.libs[libName]; ok {
		lib.Funcs = append(lib.Funcs, funcName)
	}
	m.mu.Unlock()
}

func (m *LibraryManager) Delete(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	lib, ok := m.libs[name]
	if !ok {
		return false
	}
	for _, fn := range lib.Funcs {
		delete(m.funcToLib, fn)
	}
	delete(m.libs, name)
	return true
}

func (m *LibraryManager) Flush() {
	m.mu.Lock()
	m.libs = make(map[string]*LibraryDef)
	m.funcToLib = make(map[string]string)
	m.mu.Unlock()
}

func (m *LibraryManager) SetRunning(v bool) {
	m.mu.Lock()
	m.running = v
	m.mu.Unlock()
}

func (m *LibraryManager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

func (m *LibraryManager) AllLibraries() []*LibraryDef {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*LibraryDef
	for _, lib := range m.libs {
		result = append(result, lib)
	}
	return result
}

func CompileLibraryCode(code string) ([]string, error) {
	engine, libName, ok := ParseShebang(code)
	if !ok {
		return nil, errors.New("ERR invalid shebang")
	}
	if engine != "lua" {
		return nil, errors.New("ERR unsupported engine")
	}

	L := lua.NewState(luaStateOptions())
	defer L.Close()

	OpenSafeLibs(L)

	regFuncs := make(map[string]*lua.LFunction)
	dummyCtx := &context.Context{}
	RegisterRedisLibExtended(L, dummyCtx, regFuncs, libName)

	installGlobalProtection(L)

	discard := func(ctx *context.Context, args []types.Value) types.Value {
		return types.RespOK
	}
	origDispatcher := dispatcher
	SetDispatcher(discard)
	defer SetDispatcher(origDispatcher)

	luaCode := stripShebangLine(code)
	if err := L.DoString(luaCode); err != nil {
		return nil, err
	}

	var funcNames []string
	for name := range regFuncs {
		funcNames = append(funcNames, name)
	}
	return funcNames, nil
}

func stripShebangLine(code string) string {
	idx := strings.IndexByte(code, '\n')
	if idx < 0 {
		return ""
	}
	return code[idx+1:]
}
