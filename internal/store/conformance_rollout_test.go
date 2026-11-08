package store

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func rolloutSpec(image string) ServiceSpec {
	s := webSpec()
	s.Image = image
	return s
}

func testRolloutPolicy(t *testing.T, s Store) {
	ctx := context.Background()
	svc := newService(t, s, "web", 3)
	if svc.Rollout != DefaultRolloutPolicy() || svc.Paused {
		t.Fatalf("a new service has rollout policy %+v, paused %v; want the defaults, not paused", svc.Rollout, svc.Paused)
	}
	if want := (RolloutPolicy{MaxUnavailable: 0, MaxSurge: 1, ProgressDeadlineSeconds: 600}); svc.Rollout != want {
		t.Fatalf("default policy = %+v, want %+v", svc.Rollout, want)
	}

	custom, err := s.CreateService(ctx, NewService{
		ID: "svc-api", Name: "api", Replicas: 2, Spec: webSpec(), At: testTime,
		Rollout: RolloutPolicy{MaxUnavailable: 2, MaxSurge: 0, ProgressDeadlineSeconds: 30},
	})
	noErr(t, err)
	if want := (RolloutPolicy{MaxUnavailable: 2, MaxSurge: 0, ProgressDeadlineSeconds: 30}); custom.Rollout != want {
		t.Fatalf("custom policy = %+v, want %+v", custom.Rollout, want)
	}
	got, err := s.GetService(ctx, "svc-api")
	noErr(t, err)
	if got.Rollout != custom.Rollout || got.Paused {
		t.Fatalf("stored service = %+v", got)
	}
	for name, p := range map[string]RolloutPolicy{
		"negative surge":        {MaxSurge: -1},
		"negative unavailable":  {MaxUnavailable: -1, MaxSurge: 1},
		"too much surge":        {MaxSurge: MaxRolloutReplicas + 1},
		"too long a deadline":   {MaxSurge: 1, ProgressDeadlineSeconds: MaxProgressDeadlineSeconds + 1},
		"a negative deadline":   {MaxSurge: 1, ProgressDeadlineSeconds: -5},
	} {
		_, err := s.CreateService(ctx, NewService{ID: "x", Name: "x", Spec: webSpec(), At: testTime, Rollout: p})
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error = %v, want ErrInvalid", name, err)
		}
	}
	if list, _ := s.ListServices(ctx, true); len(list) != 2 {
		t.Fatalf("a refused creation left a service behind: %+v", list)
	}

	// Changing the policy is a change of the service, not of any revision.
	at := testTime.Add(time.Minute)
	tight := RolloutPolicy{MaxUnavailable: 1, MaxSurge: 0, ProgressDeadlineSeconds: 120}
	changed, err := s.SetRollout(ctx, svc.ID, tight, false, 1, at)
	noErr(t, err)
	if changed.Rollout != tight || changed.Paused || changed.Version != 2 || !changed.UpdatedAt.Equal(at) || changed.CurrentRevision != 1 || changed.Replicas != 3 {
		t.Fatalf("changed = %+v", changed)
	}
	same, err := s.SetRollout(ctx, svc.ID, tight, false, 0, at.Add(time.Minute))
	if !errors.Is(err, ErrUnchanged) || same.Version != 2 || !same.UpdatedAt.Equal(at) {
		t.Fatalf("setting the same policy: %+v, %v; want ErrUnchanged and no new version", same, err)
	}
	_, err = s.SetRollout(ctx, svc.ID, DefaultRolloutPolicy(), false, 1, at) // read at version 1, but it is 2 now
	wantErr(t, err, ErrVersionConflict)
	if cur, _ := s.GetService(ctx, svc.ID); cur.Rollout != tight {
		t.Fatalf("a conflicting change was applied: %+v", cur)
	}
	for name, p := range map[string]RolloutPolicy{
		"neither limit":        {MaxUnavailable: 0, MaxSurge: 0, ProgressDeadlineSeconds: 60},
		"negative":             {MaxUnavailable: -1, MaxSurge: 1, ProgressDeadlineSeconds: 60},
		"too many":             {MaxUnavailable: MaxRolloutReplicas + 1, ProgressDeadlineSeconds: 60},
		"no deadline":          {MaxUnavailable: 1},
		"too long a deadline":  {MaxUnavailable: 1, ProgressDeadlineSeconds: MaxProgressDeadlineSeconds + 1},
		"a deadline below one": {MaxSurge: 1, ProgressDeadlineSeconds: 0},
	} {
		if _, err := s.SetRollout(ctx, svc.ID, p, false, 0, at); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error = %v, want ErrInvalid", name, err)
		}
	}
	_, err = s.SetRollout(ctx, "nope", tight, false, 0, at)
	wantErr(t, err, ErrNotFound)

	// Pausing is a change too, and pausing twice is not.
	paused, err := s.SetRollout(ctx, svc.ID, tight, true, 2, at.Add(2*time.Minute))
	noErr(t, err)
	if !paused.Paused || paused.Rollout != tight || paused.Version != 3 {
		t.Fatalf("paused = %+v", paused)
	}
	_, err = s.SetRollout(ctx, svc.ID, tight, true, 0, at.Add(3*time.Minute))
	wantErr(t, err, ErrUnchanged)
	resumed, err := s.SetRollout(ctx, svc.ID, tight, false, 0, at.Add(3*time.Minute))
	if err != nil || resumed.Paused || resumed.Version != 4 {
		t.Fatalf("resumed = %+v, %v", resumed, err)
	}
	// Scaling and deploying leave the policy and the pause alone.
	_, err = s.SetRollout(ctx, svc.ID, tight, true, 0, at.Add(4*time.Minute))
	noErr(t, err)
	_, err = s.ScaleService(ctx, svc.ID, 6, 0, at.Add(5*time.Minute))
	noErr(t, err)
	after, _, err := s.DeployService(ctx, svc.ID, Deployment{Spec: rolloutSpec("nginx:1.28"), At: at.Add(6 * time.Minute)})
	noErr(t, err)
	if after.Rollout != tight || !after.Paused || after.Replicas != 6 || after.CurrentRevision != 2 {
		t.Fatalf("after scaling and deploying = %+v", after)
	}
	// A deleted service cannot be configured.
	_, err = s.DeleteService(ctx, "svc-api", 0, at)
	noErr(t, err)
	_, err = s.SetRollout(ctx, "svc-api", tight, false, 0, at)
	wantErr(t, err, ErrNotFound)
}

// A rollback deploys an old spec as a new revision. The definition says which
// revisions have the current spec, so replicas of the old one are recognized.
func testDefinitionSameSpec(t *testing.T, s Store) {
	ctx := context.Background()
	svc := newService(t, s, "web", 2)
	_, _, err := s.DeployService(ctx, svc.ID, Deployment{Spec: rolloutSpec("nginx:1.28"), At: testTime})
	noErr(t, err)
	_, _, err = s.DeployService(ctx, svc.ID, Deployment{Spec: rolloutSpec("nginx:1.29"), At: testTime})
	noErr(t, err)
	_, rev4, err := s.DeployService(ctx, svc.ID, Deployment{Spec: webSpec(), Message: "rollback to revision 1", At: testTime})
	noErr(t, err)
	if rev4.Revision != 4 || rev4.Message != "rollback to revision 1" {
		t.Fatalf("revision 4 = %+v", rev4)
	}
	newService(t, s, "api", 1)

	defs, err := s.ListDefinitions(ctx, false)
	noErr(t, err)
	if len(defs) != 2 {
		t.Fatalf("definitions = %+v", defs)
	}
	api, web := defs[0], defs[1]
	if !reflect.DeepEqual(web.SameSpec, []int{1, 4}) || web.Service.CurrentRevision != 4 {
		t.Fatalf("web: same spec %v, current %d; want [1 4] and 4", web.SameSpec, web.Service.CurrentRevision)
	}
	for rev, want := range map[int]bool{1: true, 2: false, 3: false, 4: true, 5: false} {
		if got := web.Serves(rev); got != want {
			t.Errorf("web.Serves(%d) = %v, want %v", rev, got, want)
		}
	}
	if !reflect.DeepEqual(api.SameSpec, []int{1}) || !api.Serves(1) || api.Serves(2) {
		t.Fatalf("api: same spec %v", api.SameSpec)
	}
	revs, err := s.ListRevisions(ctx, svc.ID)
	noErr(t, err)
	if got := SameSpecRevisions(revs, 4); !reflect.DeepEqual(got, []int{1, 4}) {
		t.Fatalf("SameSpecRevisions = %v", got)
	}
}

func testRollouts(t *testing.T, s Store) {
	ctx := context.Background()
	web := newService(t, s, "web", 3)
	api := newService(t, s, "api", 1)
	list, err := s.ListRollouts(ctx, "")
	if err != nil || len(list) != 0 {
		t.Fatalf("rollouts of a new store = %+v, %v", list, err)
	}

	r1 := Rollout{ServiceID: web.ID, Revision: 1, State: RolloutProgressing, Updated: 1, Ready: 0, Old: 2, StartedAt: testTime, ProgressAt: testTime}
	noErr(t, s.SaveRollout(ctx, r1))
	got, err := s.ListRollouts(ctx, web.ID)
	if err != nil || len(got) != 1 || !got[0].Same(r1) {
		t.Fatalf("rollouts = %+v, %v; want %+v", got, err, r1)
	}

	// Saving again replaces the record.
	done := testTime.Add(time.Hour)
	r1.State, r1.Ready, r1.Old, r1.ProgressAt, r1.FinishedAt, r1.Paused = RolloutComplete, 3, 0, done, &done, true
	r1.Message = "revision 1 is rolled out"
	noErr(t, s.SaveRollout(ctx, r1))
	got, err = s.ListRollouts(ctx, web.ID)
	if err != nil || len(got) != 1 || !got[0].Same(r1) || got[0].FinishedAt == nil || !got[0].FinishedAt.Equal(done) {
		t.Fatalf("rollouts after a second save = %+v, %v; want %+v", got, err, r1)
	}

	// Newest revision first; one service at a time, or all.
	_, _, err = s.DeployService(ctx, web.ID, Deployment{Spec: rolloutSpec("nginx:1.28"), At: testTime})
	noErr(t, err)
	r2 := Rollout{
		ServiceID: web.ID, Revision: 2, State: RolloutFailed, Reason: RolloutDeadlineExceeded, Message: "no progress for 10m0s",
		Updated: 1, Old: 3, StartedAt: testTime, ProgressAt: testTime,
	}
	noErr(t, s.SaveRollout(ctx, r2))
	noErr(t, s.SaveRollout(ctx, Rollout{ServiceID: api.ID, Revision: 1, State: RolloutProgressing, StartedAt: testTime, ProgressAt: testTime}))
	got, err = s.ListRollouts(ctx, web.ID)
	if err != nil || len(got) != 2 || got[0].Revision != 2 || got[1].Revision != 1 || !got[0].Same(r2) {
		t.Fatalf("rollouts of web = %+v, %v", got, err)
	}
	all, err := s.ListRollouts(ctx, "")
	if err != nil || len(all) != 3 || all[0].ServiceID != api.ID || all[1].ServiceID != web.ID || all[1].Revision != 2 {
		t.Fatalf("all rollouts = %+v, %v", all, err)
	}
	if other, _ := s.ListRollouts(ctx, "nobody"); len(other) != 0 {
		t.Fatalf("rollouts of an unknown service = %+v", other)
	}

	// Malformed records and unknown revisions are refused.
	good := Rollout{ServiceID: web.ID, Revision: 2, State: RolloutProgressing, StartedAt: testTime, ProgressAt: testTime}
	for name, mutate := range map[string]func(*Rollout){
		"no service":       func(r *Rollout) { r.ServiceID = "" },
		"revision zero":    func(r *Rollout) { r.Revision = 0 },
		"unknown state":    func(r *Rollout) { r.State = "stuck" },
		"negative ready":   func(r *Rollout) { r.Ready = -1 },
		"negative updated": func(r *Rollout) { r.Updated = -1 },
		"negative old":     func(r *Rollout) { r.Old = -1 },
		"no start":         func(r *Rollout) { r.StartedAt = time.Time{} },
		"no progress time": func(r *Rollout) { r.ProgressAt = time.Time{} },
	} {
		bad := good
		mutate(&bad)
		if err := s.SaveRollout(ctx, bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error = %v, want ErrInvalid", name, err)
		}
	}
	missing := good
	missing.Revision = 9
	wantErr(t, s.SaveRollout(ctx, missing), ErrNotFound)
	missing = good
	missing.ServiceID = "nobody"
	wantErr(t, s.SaveRollout(ctx, missing), ErrNotFound)
	if all, _ := s.ListRollouts(ctx, ""); len(all) != 3 {
		t.Fatalf("a refused save changed the records: %+v", all)
	}

	// A rolled-back transaction leaves no rollout behind.
	boom := errors.New("boom")
	err = s.InTx(ctx, func(tx Tx) error {
		changed := r2
		changed.State, changed.Reason, changed.Message = RolloutSuperseded, "", ""
		noErr(t, tx.SaveRollout(ctx, changed))
		return boom
	})
	wantErr(t, err, boom)
	got, err = s.ListRollouts(ctx, web.ID)
	if err != nil || len(got) != 2 || got[0].State != RolloutFailed {
		t.Fatalf("rollouts after a rolled-back transaction = %+v, %v", got, err)
	}

	// Same compares what a record says, not how its times are written.
	zone := time.FixedZone("x", 3600)
	shifted := r2
	shifted.StartedAt = r2.StartedAt.In(zone)
	if !shifted.Same(r2) {
		t.Error("the same instant in another zone is a different record")
	}
	shifted.Ready++
	if shifted.Same(r2) {
		t.Error("records with different counts are the same")
	}
}
