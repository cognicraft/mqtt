package mqtt

import "testing"

func TestTopic(t *testing.T) {

	testCases := []struct {
		name    string
		topic   Topic
		filter  Topic
		matches bool
	}{
		{"eq", Topic("foo"), Topic("foo"), true},
		{"neq", Topic("foo"), Topic("bar"), false},
		{"multi-level-wild:01", Topic("foo"), Topic("#"), true},
		{"multi-level-wild:02", Topic("foo/bar"), Topic("#"), true},
		{"multi-level-wild:03", Topic("foo/bar"), Topic("foo/#"), true},
		{"multi-level-wild:04", Topic("foo/bar/baz/konz"), Topic("foo/#"), true},
		{"multi-level-wild:04", Topic("foo/bar/baz/konz"), Topic("foo/#/baz"), false},
		{"single-level-wild:01", Topic("foo"), Topic("+"), true},
		{"single-level-wild:02", Topic("foo/bar"), Topic("+"), false},
		{"single-level-wild:03", Topic("foo/bar"), Topic("+/bar"), true},
		{"single-level-wild:04", Topic("foo/bar"), Topic("foo/+"), true},
		{"single-level-wild:05", Topic("foo/bar/baz"), Topic("foo/+/baz"), true},
		{"single-level-wild:06", Topic("foo/zonk/baz"), Topic("foo/+/baz"), true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.filter.Accept(tc.topic)
			if tc.matches != got {
				t.Errorf("expected: %v, got: %v", tc.matches, got)
			}
		})
	}
}

func TestTopicIsValid(t *testing.T) {
	testCases := []struct {
		name  string
		topic Topic
		valid bool
	}{
		{"v:0", Topic("foo"), true},
		{"v:1", Topic("foo/bar"), true},
		{"v:2", Topic("foo/bar/baz"), true},
		{"i:1", Topic("+"), false},
		{"i:2", Topic("#"), false},
		{"i:3", Topic("foo/+/baz"), false},
		{"i:4", Topic("foo/#/baz"), false},
		{"i:5", Topic("foo/bar/+"), false},
		{"i:6", Topic("foo/bar/#"), false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.topic.IsValid()
			if tc.valid != got {
				t.Errorf("expected: %v, got: %v", tc.valid, got)
			}
		})
	}
}

func TestTopicIsFilter(t *testing.T) {
	testCases := []struct {
		name   string
		topic  Topic
		filter bool
	}{
		{"f:0", Topic("foo"), true},
		{"f:1", Topic("foo/bar"), true},
		{"f:2", Topic("foo/bar/baz"), true},
		{"+", Topic("+"), true},
		{"#", Topic("#"), true},
		{"f:5", Topic("foo/+/baz"), true},
		{"f:6", Topic("foo/bar/+"), true},
		{"f:7", Topic("foo/bar/#"), true},
		{"nf:0", Topic("foo/#/baz"), false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.topic.IsFilter()
			if tc.filter != got {
				t.Errorf("expected: %v, got: %v", tc.filter, got)
			}
		})
	}
}
