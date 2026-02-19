package manager

import (
	"cf_arbitrage/mock/mapp"
	"testing"

	"github.com/golang/mock/gomock"
)

func TestManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	a1 := "1"
	c1 := []byte("1")
	r1 := false

	a2 := "2"
	c2 := []byte("2")
	r2 := false

	mApp1 := mapp.NewMockApp(ctrl)
	mApp1.EXPECT().UpdateConfigJSON(c1).Return(nil)
	mApp1.EXPECT().UpdateConfigJSON(c2).Return(nil)
	mApp1.EXPECT().Start().DoAndReturn(func() error {
		r1 = true
		return nil
	})
	mApp1.EXPECT().Stop().DoAndReturn(func() error {
		r1 = false
		return nil
	})
	mApp1.EXPECT().GetConfig().Return(1, nil)
	mApp1.EXPECT().Detail().Return(2, nil)
	mApp1.EXPECT().Running().DoAndReturn(func() bool {
		return r1
	}).AnyTimes()

	mApp2 := mapp.NewMockApp(ctrl)
	mApp2.EXPECT().UpdateConfigJSON(c2).Return(nil)
	mApp2.EXPECT().Start().DoAndReturn(func() error {
		r2 = true
		return nil
	})
	mApp2.EXPECT().Stop().DoAndReturn(func() error {
		r2 = false
		return nil
	})
	mApp2.EXPECT().GetConfig().Return(2, nil)
	mApp2.EXPECT().Detail().Return(3, nil)
	mApp2.EXPECT().Running().DoAndReturn(func() bool {
		return r2
	}).AnyTimes()

	mFactory := mapp.NewMockFactory(ctrl)
	mFactory.EXPECT().Create(a1).Return(mApp1, nil)
	mFactory.EXPECT().Create(a2).Return(mApp2, nil)

	manager := NewManager(mFactory)

	if err := manager.Add(a1, c1); err != nil {
		t.Fatalf("add a1 fail %s", err.Error())
	}
	if err := manager.Add(a2, c2); err != nil {
		t.Fatalf("add a2 fail %s", err.Error())
	}

	if err := manager.StartAll(); err != nil {
		t.Fatalf("start all fail error=%s", err.Error())
	}

	if v, _ := manager.Get(a1); v.Key != a1 || v.Config != 1 || v.Detail != 2 {
		t.Errorf("test get a1 fail %+v", *v)
	}
	if v, _ := manager.Get(a2); v.Key != a2 || v.Config != 2 || v.Detail != 3 {
		t.Errorf("test get a2 fail %+v", *v)
	}

	if err := manager.Add(a1, c1); err == nil {
		t.Fatalf("expect duplicate add fail")
	}

	if err := manager.Update(a1, c2); err != nil {
		t.Fatalf("update a1 fail error=%s", err.Error())
	}

	if err := manager.Stop(a1); err != nil {
		t.Fatalf("a1 stop fail error=%s", err.Error())
	}

	if err := manager.Stop(a2); err != nil {
		t.Fatalf("a2 stop fail error=%s", err.Error())
	}

	if err := manager.StopAll(); err != nil {
		t.Fatalf("stop all fail error=%s", err.Error())
	}

}
