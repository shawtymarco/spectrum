package animation

import (
	"testing"

	"github.com/sandertv/gophertunnel/minecraft"
)

type recordingAnimation struct {
	begin, end, clear int
}

func (*recordingAnimation) Play(*minecraft.Conn, minecraft.GameData) {}
func (a *recordingAnimation) Clear(*minecraft.Conn, minecraft.GameData) {
	a.clear++
}
func (a *recordingAnimation) BeginClear(*minecraft.Conn, minecraft.GameData) {
	a.begin++
}
func (a *recordingAnimation) EndClear(*minecraft.Conn, minecraft.GameData) {
	a.end++
}

type recordingLegacyAnimation struct{ clear int }

func (*recordingLegacyAnimation) Play(*minecraft.Conn, minecraft.GameData) {}
func (a *recordingLegacyAnimation) Clear(*minecraft.Conn, minecraft.GameData) {
	a.clear++
}

func TestPhasedClearUsesSeparateBoundaries(t *testing.T) {
	a := &recordingAnimation{}
	BeginClear(a, nil, minecraft.GameData{})
	EndClear(a, nil, minecraft.GameData{})
	if a.begin != 1 || a.end != 1 || a.clear != 0 {
		t.Fatalf("phased calls = begin:%d end:%d clear:%d", a.begin, a.end, a.clear)
	}
}

func TestLegacyClearRunsOnlyAtEnd(t *testing.T) {
	a := &recordingLegacyAnimation{}
	BeginClear(a, nil, minecraft.GameData{})
	if a.clear != 0 {
		t.Fatal("legacy animation cleared before backend state")
	}
	EndClear(a, nil, minecraft.GameData{})
	if a.clear != 1 {
		t.Fatalf("legacy clear calls = %d, want 1", a.clear)
	}
}

func TestAcknowledgedDimensionRequiresClientBoundary(t *testing.T) {
	if !NeedsAcknowledgement(&AcknowledgedDimension{}) {
		t.Fatal("acknowledged dimension animation did not expose its client boundary")
	}
	if NeedsAcknowledgement(&Dimension{}) {
		t.Fatal("server-acknowledged dimension animation unexpectedly waits for the client")
	}
}

func TestAlternateDimensionDiffersFromTarget(t *testing.T) {
	for _, target := range []int32{0, 1, 2} {
		if got := alternateDimension(target); got == target {
			t.Fatalf("alternate dimension for %d remained unchanged", target)
		}
	}
}
