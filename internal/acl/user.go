package acl

import (
	"strings"
	"sync"
)

type User struct {
	Name        string
	On          bool
	Passwords   []string
	Commands    map[string]bool
	Categories  map[string]bool
	Keys        []string
	AllKeys     bool
	AllCommands bool
}

func NewUser(name string) *User {
	return &User{
		Name:       name,
		Passwords:  []string{},
		Commands:   make(map[string]bool),
		Categories: make(map[string]bool),
		Keys:       []string{},
	}
}

func (u *User) AddRule(rule string) {
	if rule == "on" {
		u.On = true
	} else if rule == "off" {
		u.On = false
	} else if strings.HasPrefix(rule, ">") {
		u.Passwords = append(u.Passwords, rule[1:])
	} else if strings.HasPrefix(rule, "~") {
		u.Keys = append(u.Keys, rule[1:])
	} else if strings.HasPrefix(rule, "+@") {
		u.Categories[strings.ToLower(rule[2:])] = true
	} else if strings.HasPrefix(rule, "+") {
		u.Commands[strings.ToLower(rule[1:])] = true
	} else if rule == "allkeys" {
		u.AllKeys = true
	} else if rule == "allcommands" {
		u.AllCommands = true
	}
}

func (u *User) Authenticate(password string) bool {
	if !u.On {
		return false
	}
	if len(u.Passwords) == 0 {
		return true // nopass
	}
	for _, p := range u.Passwords {
		if p == password {
			return true
		}
	}
	return false
}

func (u *User) CanExecute(cmd string) bool {
	if u.AllCommands || u.Categories["all"] {
		return true
	}
	cmd = strings.ToLower(cmd)
	if u.Commands[cmd] {
		return true
	}
	if u.Categories["scripting"] {
		if cmd == "eval" || cmd == "evalsha" || cmd == "script" {
			return true
		}
	}
	return false
}

func (u *User) CanAccessKey(key string) bool {
	if u.AllKeys {
		return true
	}
	for _, pat := range u.Keys {
		if MatchGlob(pat, key) {
			return true
		}
	}
	return false
}

func MatchGlob(pattern, subject string) bool {
	if strings.HasSuffix(pattern, "*") {
		prefix := pattern[:len(pattern)-1]
		return strings.HasPrefix(subject, prefix)
	}
	return pattern == subject
}

var (
	mu    sync.RWMutex
	users = map[string]*User{
		"default": {Name: "default", On: true, AllCommands: true, AllKeys: true},
	}
)

func GetUser(name string) *User {
	mu.RLock()
	defer mu.RUnlock()
	return users[name]
}

func SetUser(u *User) {
	mu.Lock()
	defer mu.Unlock()
	users[u.Name] = u
}
