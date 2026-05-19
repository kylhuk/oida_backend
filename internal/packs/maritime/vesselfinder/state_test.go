package vesselfinder

import (
	"reflect"
	"testing"
	"time"
)

func TestBuildPageJobsRandomizesDimensionsButKeepsPagesAscending(t *testing.T) {
	jobs := BuildPageJobs(
		[]Dimension{{Code: "PA", Label: "Panama"}, {Code: "US", Label: "United States"}},
		[]Dimension{{Code: "2", Label: "Tankers"}, {Code: "7", Label: "Cargo"}},
		3,
		42,
	)
	if again := BuildPageJobs(
		[]Dimension{{Code: "PA", Label: "Panama"}, {Code: "US", Label: "United States"}},
		[]Dimension{{Code: "2", Label: "Tankers"}, {Code: "7", Label: "Cargo"}},
		3,
		42,
	); !reflect.DeepEqual(jobs, again) {
		t.Fatalf("expected deterministic randomization\n got: %#v\nwant: %#v", again, jobs)
	}
	seenDimensions := map[string]bool{}
	for idx := 0; idx < len(jobs); idx += 3 {
		if idx+2 >= len(jobs) {
			t.Fatalf("dimension group is incomplete at index %d: %#v", idx, jobs)
		}
		first := jobs[idx]
		key := first.CountryCode + "/" + first.TypeCode
		if seenDimensions[key] {
			t.Fatalf("dimension %s appeared in more than one group: %#v", key, jobs)
		}
		seenDimensions[key] = true
		for offset, wantPage := range []int{1, 2, 3} {
			job := jobs[idx+offset]
			if job.CountryCode != first.CountryCode || job.TypeCode != first.TypeCode || job.Page != wantPage {
				t.Fatalf("pages must stay ascending within a dimension group, got group %#v", jobs[idx:idx+3])
			}
		}
	}
}

func TestTerminal404SkipsHigherPagesForDimension(t *testing.T) {
	terminals := []Terminal404{{CountryCode: "PA", TypeCode: "2", Page: 3}}
	if ShouldSkipPage(PageJob{CountryCode: "PA", TypeCode: "2", Page: 4}, terminals) != true {
		t.Fatal("expected page after terminal 404 to be skipped")
	}
	if ShouldSkipPage(PageJob{CountryCode: "PA", TypeCode: "2", Page: 3}, terminals) != false {
		t.Fatal("expected terminal page itself not to be skipped")
	}
	if ShouldSkipPage(PageJob{CountryCode: "US", TypeCode: "2", Page: 4}, terminals) != false {
		t.Fatal("expected other dimensions not to be skipped")
	}
}

func TestListPageOutcomeMarksEmptyPagesTerminal(t *testing.T) {
	status, terminal := ListPageOutcome(200, nil)
	if status != "empty" || !terminal {
		t.Fatalf("empty page got status=%q terminal=%t, want empty terminal", status, terminal)
	}
	status, terminal = ListPageOutcome(404, []string{"https://www.vesselfinder.com/vessels/details/1"})
	if status != "terminal_404" || !terminal {
		t.Fatalf("404 page got status=%q terminal=%t, want terminal_404 terminal", status, terminal)
	}
	status, terminal = ListPageOutcome(200, []string{"https://www.vesselfinder.com/vessels/details/1"})
	if status != "success" || terminal {
		t.Fatalf("page with links got status=%q terminal=%t, want success nonterminal", status, terminal)
	}
}

func TestClaimOldestScanQueue(t *testing.T) {
	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	queue := []ScanQueueItem{
		{DetailURL: "new", NextScanAt: base.Add(2 * time.Hour), Status: "pending"},
		{DetailURL: "failed", NextScanAt: base.Add(-time.Hour), Status: "failed"},
		{DetailURL: "old", NextScanAt: base.Add(-2 * time.Hour), Status: "pending"},
		{DetailURL: "leased", NextScanAt: base.Add(-3 * time.Hour), Status: "leased"},
	}
	claimed, updated := ClaimOldest(queue, base, "lease-1", 2)
	if got, want := []string{claimed[0].DetailURL, claimed[1].DetailURL}, []string{"old", "failed"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("claim order mismatch got %#v want %#v", got, want)
	}
	for _, item := range updated {
		if item.DetailURL == "old" || item.DetailURL == "failed" {
			if item.Status != "leased" || item.LeaseID != "lease-1" {
				t.Fatalf("expected claimed item to be leased: %#v", item)
			}
		}
	}
}

func TestApplyScanResultSchedulesSuccessAndFailure(t *testing.T) {
	now := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	item := ScanQueueItem{DetailURL: "detail", Status: "leased", AttemptCount: 2}
	success := ApplyScanResult(item, ScanResult{StatusCode: 200, Success: true}, now, time.Hour)
	if success.Status != "pending" || success.AttemptCount != 0 || !success.LastScannedAt.Equal(now) || !success.NextScanAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("unexpected success result: %#v", success)
	}
	failed := ApplyScanResult(item, ScanResult{StatusCode: 503, Success: false, ErrorCode: "browser_timeout"}, now, time.Hour)
	if failed.Status != "failed" || failed.AttemptCount != 3 || failed.LastErrorCode != "browser_timeout" || !failed.NextScanAt.After(now) {
		t.Fatalf("unexpected failure result: %#v", failed)
	}
}
