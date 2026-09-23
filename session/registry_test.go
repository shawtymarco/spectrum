package session

import "testing"

func TestSessionClosePreservesOtherConnectionForSameAccount(t *testing.T) {
	registry := NewRegistry()
	original, replacement, rejected := &Session{}, &Session{}, &Session{}
	registry.AddSession("account", original)
	registry.RemoveSessionIfCurrent("account", rejected)
	if registry.GetSession("account") != original {
		t.Fatal("rejected login removed the existing session")
	}
	registry.AddSession("account", replacement)
	registry.RemoveSessionIfCurrent("account", original)
	if registry.GetSession("account") != replacement {
		t.Fatal("old disconnect removed the replacement session")
	}
	registry.RemoveSessionIfCurrent("account", replacement)
	if registry.GetSession("account") != nil || len(registry.GetSessions()) != 0 {
		t.Fatal("closed current session remained in discovery")
	}
}
