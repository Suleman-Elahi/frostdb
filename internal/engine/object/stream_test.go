package object

import "testing"

func TestStreamAddAndReadGroup(t *testing.T) {
	stream := NewStream()

	id1, err := stream.Add("1-0", []StreamField{{Name: "field", Value: "one"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	id2, err := stream.Add("2-0", []StreamField{{Name: "field", Value: "two"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id1 != "1-0" || id2 != "2-0" {
		t.Fatalf("unexpected ids: %s %s", id1, id2)
	}

	if err := stream.CreateGroup("g1", "0-0"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	entries, err := stream.ReadGroup("g1", "c1", 2, ">", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].ID.String() != "1-0" || entries[1].ID.String() != "2-0" {
		t.Fatalf("unexpected entry order: %+v", entries)
	}

	count, smallest, largest, consumers := stream.PendingSummary("g1")
	if count != 2 || smallest != "1-0" || largest != "2-0" {
		t.Fatalf("unexpected pending summary: %d %s %s", count, smallest, largest)
	}
	if len(consumers) != 1 || consumers[0].Name != "c1" || consumers[0].Pending != 2 {
		t.Fatalf("unexpected consumer summary: %+v", consumers)
	}

	acked := stream.Ack("g1", "1-0")
	if acked != 1 {
		t.Fatalf("expected one ack, got %d", acked)
	}
	count, _, _, _ = stream.PendingSummary("g1")
	if count != 1 {
		t.Fatalf("expected one pending after ack, got %d", count)
	}
}

func TestStreamAutoClaim(t *testing.T) {
	stream := NewStream()
	if err := stream.CreateGroup("g1", "0-0"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, _ = stream.Add("1-0", []StreamField{{Name: "field", Value: "one"}})
	_, _ = stream.Add("2-0", []StreamField{{Name: "field", Value: "two"}})
	_, err := stream.ReadGroup("g1", "c1", 2, ">", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	next, claimed, deleted := stream.AutoClaim("g1", "c2", 0, "0-0", 10, false)
	if next != "2-0" {
		t.Fatalf("unexpected next cursor: %s", next)
	}
	if len(claimed) != 2 || len(deleted) != 0 {
		t.Fatalf("unexpected claim result: %d claimed, %d deleted", len(claimed), len(deleted))
	}
	if claimed[0].Consumer != "c2" || claimed[1].Consumer != "c2" {
		t.Fatalf("expected consumer to be updated: %+v", claimed)
	}
}

