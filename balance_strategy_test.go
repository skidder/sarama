package sarama

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestBalanceStrategyRange(t *testing.T) {
	tests := []struct {
		members  map[string][]string
		topics   map[string][]int32
		expected BalanceStrategyPlan
	}{
		{
			members: map[string][]string{"M1": {"T1", "T2"}, "M2": {"T1", "T2"}},
			topics:  map[string][]int32{"T1": {0, 1, 2, 3}, "T2": {0, 1, 2, 3}},
			expected: BalanceStrategyPlan{
				"M1": map[string][]int32{"T1": {0, 1}, "T2": {2, 3}},
				"M2": map[string][]int32{"T1": {2, 3}, "T2": {0, 1}},
			},
		},
		{
			members: map[string][]string{"M1": {"T1", "T2"}, "M2": {"T1", "T2"}},
			topics:  map[string][]int32{"T1": {0, 1, 2}, "T2": {0, 1, 2}},
			expected: BalanceStrategyPlan{
				"M1": map[string][]int32{"T1": {0, 1}, "T2": {2}},
				"M2": map[string][]int32{"T1": {2}, "T2": {0, 1}},
			},
		},
		{
			members: map[string][]string{"M1": {"T1"}, "M2": {"T1", "T2"}},
			topics:  map[string][]int32{"T1": {0, 1}, "T2": {0, 1}},
			expected: BalanceStrategyPlan{
				"M1": map[string][]int32{"T1": {0}},
				"M2": map[string][]int32{"T1": {1}, "T2": {0, 1}},
			},
		},
	}

	strategy := BalanceStrategyRange
	if strategy.Name() != "range" {
		t.Errorf("Unexpected stategy name\nexpected: range\nactual: %v", strategy.Name())
	}

	for _, test := range tests {
		members := make(map[string]ConsumerGroupMemberMetadata)
		for memberID, topics := range test.members {
			members[memberID] = ConsumerGroupMemberMetadata{Topics: topics}
		}

		actual, err := strategy.Plan(members, test.topics)
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		} else if !reflect.DeepEqual(actual, test.expected) {
			t.Errorf("Plan does not match expectation\nexpected: %#v\nactual: %#v", test.expected, actual)
		}
	}
}

func TestBalanceStrategyRoundRobin(t *testing.T) {
	tests := []struct {
		members  map[string][]string
		topics   map[string][]int32
		expected BalanceStrategyPlan
	}{
		{
			members: map[string][]string{"M1": {"T1", "T2"}, "M2": {"T1", "T2"}},
			topics:  map[string][]int32{"T1": {0, 1, 2, 3}, "T2": {0, 1, 2, 3}},
			expected: BalanceStrategyPlan{
				"M1": map[string][]int32{"T1": {0, 2}, "T2": {1, 3}},
				"M2": map[string][]int32{"T1": {1, 3}, "T2": {0, 2}},
			},
		},
		{
			members: map[string][]string{"M1": {"T1", "T2"}, "M2": {"T1", "T2"}},
			topics:  map[string][]int32{"T1": {0, 1, 2}, "T2": {0, 1, 2}},
			expected: BalanceStrategyPlan{
				"M1": map[string][]int32{"T1": {0, 2}, "T2": {1}},
				"M2": map[string][]int32{"T1": {1}, "T2": {0, 2}},
			},
		},
	}

	strategy := BalanceStrategyRoundRobin
	if strategy.Name() != "roundrobin" {
		t.Errorf("Unexpected stategy name\nexpected: range\nactual: %v", strategy.Name())
	}

	for _, test := range tests {
		members := make(map[string]ConsumerGroupMemberMetadata)
		for memberID, topics := range test.members {
			members[memberID] = ConsumerGroupMemberMetadata{Topics: topics}
		}

		actual, err := strategy.Plan(members, test.topics)
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		} else if !reflect.DeepEqual(actual, test.expected) {
			t.Errorf("Plan does not match expectation\nexpected: %#v\nactual: %#v", test.expected, actual)
		}
	}
}

func Test_deserializeTopicPartitionAssignment(t *testing.T) {
	type args struct {
		userDataBytes []byte
	}
	tests := []struct {
		name    string
		args    args
		want    StickyAssignorUserData
		wantErr bool
	}{
		{
			name: "Nil userdata bytes",
			args: args{},
			want: &StickyAssignorUserDataV1{},
		},
		{
			name: "Non-empty invalid userdata bytes",
			args: args{
				userDataBytes: []byte{
					0x00, 0x00,
					0x00, 0x00, 0x00, 0x01,
					0x00, 0x03, 'f', 'o', 'o',
				},
			},
			wantErr: true,
		},
		{
			name: "Valid v0 userdata bytes",
			args: args{
				userDataBytes: []byte{
					0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x74, 0x30,
					0x33, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
					0x05,
				},
			},
			want: &StickyAssignorUserDataV0{
				Topics: map[string][]int32{"t03": {5}},
				topicPartitions: []topicPartitionAssignment{
					topicPartitionAssignment{
						Topic:     "t03",
						Partition: 5,
					},
				},
			},
		},
		{
			name: "Valid v1 userdata bytes",
			args: args{
				userDataBytes: []byte{
					0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x74, 0x30,
					0x36, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff,
					0xff,
				},
			},
			want: &StickyAssignorUserDataV1{
				Topics:     map[string][]int32{"t06": {0, 4}},
				Generation: -1,
				topicPartitions: []topicPartitionAssignment{
					topicPartitionAssignment{
						Topic:     "t06",
						Partition: 0,
					},
					topicPartitionAssignment{
						Topic:     "t06",
						Partition: 4,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := deserializeTopicPartitionAssignment(tt.args.userDataBytes)
			if (err != nil) != tt.wantErr {
				t.Errorf("deserializeTopicPartitionAssignment() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("deserializeTopicPartitionAssignment() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_prepopulateCurrentAssignments(t *testing.T) {
	type args struct {
		members map[string]ConsumerGroupMemberMetadata
	}
	tests := []struct {
		name                   string
		args                   args
		wantCurrentAssignments map[string][]topicPartitionAssignment
		wantPrevAssignments    map[topicPartitionAssignment]consumerGenerationPair
		wantErr                bool
	}{
		{
			name:                   "Empty map",
			wantCurrentAssignments: map[string][]topicPartitionAssignment{},
			wantPrevAssignments:    map[topicPartitionAssignment]consumerGenerationPair{},
		},
		{
			name: "Single consumer",
			args: args{
				members: map[string]ConsumerGroupMemberMetadata{
					"c01": ConsumerGroupMemberMetadata{
						Version: 2,
						UserData: []byte{
							0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x74, 0x30,
							0x36, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
							0x00, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff,
							0xff,
						},
					},
				},
			},
			wantCurrentAssignments: map[string][]topicPartitionAssignment{
				"c01": []topicPartitionAssignment{
					topicPartitionAssignment{
						Topic:     "t06",
						Partition: 0,
					},
					topicPartitionAssignment{
						Topic:     "t06",
						Partition: 4,
					},
				},
			},
			wantPrevAssignments: map[topicPartitionAssignment]consumerGenerationPair{},
		},
		{
			name: "Duplicate consumer assignments in metadata",
			args: args{
				members: map[string]ConsumerGroupMemberMetadata{
					"c01": ConsumerGroupMemberMetadata{
						Version: 2,
						UserData: []byte{
							0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x74, 0x30,
							0x36, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
							0x00, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff,
							0xff,
						},
					},
					"c02": ConsumerGroupMemberMetadata{
						Version: 2,
						UserData: []byte{
							0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x74, 0x30,
							0x36, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
							0x00, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff,
							0xff,
						},
					},
				},
			},
			wantCurrentAssignments: map[string][]topicPartitionAssignment{
				"c01": []topicPartitionAssignment{
					topicPartitionAssignment{
						Topic:     "t06",
						Partition: 0,
					},
					topicPartitionAssignment{
						Topic:     "t06",
						Partition: 4,
					},
				},
			},
			wantPrevAssignments: map[topicPartitionAssignment]consumerGenerationPair{},
		},
		{
			name: "Different generations (5, 6) of consumer assignments in metadata",
			args: args{
				members: map[string]ConsumerGroupMemberMetadata{
					"c01": ConsumerGroupMemberMetadata{
						Version: 2,
						UserData: []byte{
							0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x74, 0x30,
							0x36, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
							0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
							0x05,
						},
					},
					"c02": ConsumerGroupMemberMetadata{
						Version: 2,
						UserData: []byte{
							0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x74, 0x30,
							0x36, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
							0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
							0x06,
						},
					},
				},
			},
			wantCurrentAssignments: map[string][]topicPartitionAssignment{
				"c01": []topicPartitionAssignment{
					topicPartitionAssignment{
						Topic:     "t06",
						Partition: 0,
					},
					topicPartitionAssignment{
						Topic:     "t06",
						Partition: 4,
					},
				},
			},
			wantPrevAssignments: map[topicPartitionAssignment]consumerGenerationPair{
				topicPartitionAssignment{
					Topic:     "t06",
					Partition: 0,
				}: consumerGenerationPair{
					Generation: 5,
					MemberID:   "c01",
				},
				topicPartitionAssignment{
					Topic:     "t06",
					Partition: 4,
				}: consumerGenerationPair{
					Generation: 5,
					MemberID:   "c01",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, gotPrevAssignments, err := prepopulateCurrentAssignments(tt.args.members)

			if (err != nil) != tt.wantErr {
				t.Errorf("prepopulateCurrentAssignments() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !reflect.DeepEqual(gotPrevAssignments, tt.wantPrevAssignments) {
				t.Errorf("deserializeTopicPartitionAssignment() prevAssignments = %v, want %v", gotPrevAssignments, tt.wantPrevAssignments)
			}
		})
	}
}

func Test_areSubscriptionsIdentical(t *testing.T) {
	type args struct {
		partition2AllPotentialConsumers map[topicPartitionAssignment][]string
		consumer2AllPotentialPartitions map[string][]topicPartitionAssignment
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Empty consumers and partitions",
			args: args{
				partition2AllPotentialConsumers: make(map[topicPartitionAssignment][]string),
				consumer2AllPotentialPartitions: make(map[string][]topicPartitionAssignment),
			},
			want: true,
		},
		{
			name: "Topic partitions with identical consumer entries",
			args: args{
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: []string{"c1", "c2", "c3"},
					topicPartitionAssignment{Topic: "t1", Partition: 1}: []string{"c1", "c2", "c3"},
					topicPartitionAssignment{Topic: "t1", Partition: 2}: []string{"c1", "c2", "c3"},
				},
				consumer2AllPotentialPartitions: make(map[string][]topicPartitionAssignment),
			},
			want: true,
		},
		{
			name: "Topic partitions with mixed up consumer entries",
			args: args{
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: []string{"c1", "c2", "c3"},
					topicPartitionAssignment{Topic: "t1", Partition: 1}: []string{"c2", "c3", "c1"},
					topicPartitionAssignment{Topic: "t1", Partition: 2}: []string{"c3", "c1", "c2"},
				},
				consumer2AllPotentialPartitions: make(map[string][]topicPartitionAssignment),
			},
			want: true,
		},
		{
			name: "Topic partitions with different consumer entries",
			args: args{
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: []string{"c1", "c2", "c3"},
					topicPartitionAssignment{Topic: "t1", Partition: 1}: []string{"c2", "c3", "c1"},
					topicPartitionAssignment{Topic: "t1", Partition: 2}: []string{"cX", "c1", "c2"},
				},
				consumer2AllPotentialPartitions: make(map[string][]topicPartitionAssignment),
			},
			want: false,
		},
		{
			name: "Topic partitions with different number of consumer entries",
			args: args{
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: []string{"c1", "c2", "c3"},
					topicPartitionAssignment{Topic: "t1", Partition: 1}: []string{"c2", "c3", "c1"},
					topicPartitionAssignment{Topic: "t1", Partition: 2}: []string{"c1", "c2"},
				},
				consumer2AllPotentialPartitions: make(map[string][]topicPartitionAssignment),
			},
			want: false,
		},
		{
			name: "Consumers with identical topic partitions",
			args: args{
				partition2AllPotentialConsumers: make(map[topicPartitionAssignment][]string),
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
					"c2": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
					"c3": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
				},
			},
			want: true,
		},
		{
			name: "Consumer2 with mixed up consumer entries",
			args: args{
				partition2AllPotentialConsumers: make(map[topicPartitionAssignment][]string),
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
					"c2": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}, topicPartitionAssignment{Topic: "t1", Partition: 0}},
					"c3": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 2}, topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}},
				},
			},
			want: true,
		},
		{
			name: "Consumer2 with different consumer entries",
			args: args{
				partition2AllPotentialConsumers: make(map[topicPartitionAssignment][]string),
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
					"c2": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}, topicPartitionAssignment{Topic: "t1", Partition: 0}},
					"c3": []topicPartitionAssignment{topicPartitionAssignment{Topic: "tX", Partition: 2}, topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}},
				},
			},
			want: false,
		},
		{
			name: "Consumer2 with different number of consumer entries",
			args: args{
				partition2AllPotentialConsumers: make(map[topicPartitionAssignment][]string),
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
					"c2": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}, topicPartitionAssignment{Topic: "t1", Partition: 0}},
					"c3": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := areSubscriptionsIdentical(tt.args.partition2AllPotentialConsumers, tt.args.consumer2AllPotentialPartitions); got != tt.want {
				t.Errorf("areSubscriptionsIdentical() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_sortMemberIDsByPartitionAssignments(t *testing.T) {
	type args struct {
		assignments map[string][]topicPartitionAssignment
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "Null assignments",
			want: make([]string, 0),
		},
		{
			name: "Single assignment",
			args: args{
				assignments: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 0},
						topicPartitionAssignment{Topic: "t1", Partition: 1},
						topicPartitionAssignment{Topic: "t1", Partition: 2},
					},
				},
			},
			want: []string{"c1"},
		},
		{
			name: "Multiple assignments with different partition counts",
			args: args{
				assignments: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 0},
					},
					"c2": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 1},
						topicPartitionAssignment{Topic: "t1", Partition: 2},
					},
					"c3": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 3},
						topicPartitionAssignment{Topic: "t1", Partition: 4},
						topicPartitionAssignment{Topic: "t1", Partition: 5},
					},
				},
			},
			want: []string{"c1", "c2", "c3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sortMemberIDsByPartitionAssignments(tt.args.assignments); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sortMemberIDsByPartitionAssignments() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_sortPartitions(t *testing.T) {
	type args struct {
		currentAssignment                          map[string][]topicPartitionAssignment
		partitionsWithADifferentPreviousAssignment map[topicPartitionAssignment]consumerGenerationPair
		isFreshAssignment                          bool
		partition2AllPotentialConsumers            map[topicPartitionAssignment][]string
		consumer2AllPotentialPartitions            map[string][]topicPartitionAssignment
	}
	tests := []struct {
		name string
		args args
		want []topicPartitionAssignment
	}{
		{
			name: "Empty everything",
			want: make([]topicPartitionAssignment, 0),
		},
		{
			name: "Base case",
			args: args{
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}},
					"c2": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 1}},
					"c3": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 2}},
				},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
					"c2": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
					"c3": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: []string{"c1", "c2", "c3"},
					topicPartitionAssignment{Topic: "t1", Partition: 1}: []string{"c2", "c3", "c1"},
					topicPartitionAssignment{Topic: "t1", Partition: 2}: []string{"c3", "c1", "c2"},
				},
			},
		},
		{
			name: "Partitions assigned to a different consumer last time",
			args: args{
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}},
				},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
					"c2": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
					"c3": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: []string{"c1", "c2", "c3"},
					topicPartitionAssignment{Topic: "t1", Partition: 1}: []string{"c2", "c3", "c1"},
					topicPartitionAssignment{Topic: "t1", Partition: 2}: []string{"c3", "c1", "c2"},
				},
				partitionsWithADifferentPreviousAssignment: map[topicPartitionAssignment]consumerGenerationPair{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: consumerGenerationPair{Generation: 1, MemberID: "c2"},
				},
			},
		},
		{
			name: "Partitions assigned to a different consumer last time",
			args: args{
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}},
					"c2": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 1}},
				},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
					"c2": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
					"c3": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: []string{"c1", "c2", "c3"},
					topicPartitionAssignment{Topic: "t1", Partition: 1}: []string{"c2", "c3", "c1"},
					topicPartitionAssignment{Topic: "t1", Partition: 2}: []string{"c3", "c1", "c2"},
				},
				partitionsWithADifferentPreviousAssignment: map[topicPartitionAssignment]consumerGenerationPair{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: consumerGenerationPair{Generation: 1, MemberID: "c2"},
				},
			},
		},
		{
			name: "Fresh assignment",
			args: args{
				isFreshAssignment: true,
				currentAssignment: map[string][]topicPartitionAssignment{},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
					"c2": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
					"c3": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}, topicPartitionAssignment{Topic: "t1", Partition: 1}, topicPartitionAssignment{Topic: "t1", Partition: 2}},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: []string{"c1", "c2", "c3"},
					topicPartitionAssignment{Topic: "t1", Partition: 1}: []string{"c2", "c3", "c1"},
					topicPartitionAssignment{Topic: "t1", Partition: 2}: []string{"c3", "c1", "c2"},
				},
				partitionsWithADifferentPreviousAssignment: map[topicPartitionAssignment]consumerGenerationPair{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: consumerGenerationPair{Generation: 1, MemberID: "c2"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sortPartitions(tt.args.currentAssignment, tt.args.partitionsWithADifferentPreviousAssignment, tt.args.isFreshAssignment, tt.args.partition2AllPotentialConsumers, tt.args.consumer2AllPotentialPartitions)
			if tt.want != nil && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sortPartitions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_filterAssignedPartitions(t *testing.T) {
	type args struct {
		currentAssignment               map[string][]topicPartitionAssignment
		partition2AllPotentialConsumers map[topicPartitionAssignment][]string
	}
	tests := []struct {
		name string
		args args
		want map[string][]topicPartitionAssignment
	}{
		{
			name: "All partitions accounted for",
			args: args{
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}},
					"c2": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 1}},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: []string{"c1"},
					topicPartitionAssignment{Topic: "t1", Partition: 1}: []string{"c2"},
				},
			},
			want: map[string][]topicPartitionAssignment{
				"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}},
				"c2": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 1}},
			},
		},
		{
			name: "One consumer using an unrecognized partition",
			args: args{
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}},
					"c2": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 1}},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: []string{"c1"},
				},
			},
			want: map[string][]topicPartitionAssignment{
				"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}},
				"c2": []topicPartitionAssignment{},
			},
		},
		{
			name: "Interleaved consumer removal",
			args: args{
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}},
					"c2": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 1}},
					"c3": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 2}},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: []string{"c1"},
					topicPartitionAssignment{Topic: "t1", Partition: 2}: []string{"c3"},
				},
			},
			want: map[string][]topicPartitionAssignment{
				"c1": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 0}},
				"c2": []topicPartitionAssignment{},
				"c3": []topicPartitionAssignment{topicPartitionAssignment{Topic: "t1", Partition: 2}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filterAssignedPartitions(tt.args.currentAssignment, tt.args.partition2AllPotentialConsumers); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("filterAssignedPartitions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_canConsumerParticipateInReassignment(t *testing.T) {
	type args struct {
		memberID                        string
		currentAssignment               map[string][]topicPartitionAssignment
		consumer2AllPotentialPartitions map[string][]topicPartitionAssignment
		partition2AllPotentialConsumers map[topicPartitionAssignment][]string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Consumer has been assigned partitions not available to it",
			args: args{
				memberID: "c1",
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 0},
						topicPartitionAssignment{Topic: "t1", Partition: 1},
						topicPartitionAssignment{Topic: "t1", Partition: 2},
					},
					"c2": []topicPartitionAssignment{},
				},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 0},
						topicPartitionAssignment{Topic: "t1", Partition: 1},
					},
					"c2": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 0},
						topicPartitionAssignment{Topic: "t1", Partition: 1},
						topicPartitionAssignment{Topic: "t1", Partition: 2},
					},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: []string{"c1", "c2"},
					topicPartitionAssignment{Topic: "t1", Partition: 1}: []string{"c1", "c2"},
					topicPartitionAssignment{Topic: "t1", Partition: 2}: []string{"c2"},
				},
			},
			want: true,
		},
		{
			name: "Consumer has been assigned all available partitions",
			args: args{
				memberID: "c1",
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 0},
						topicPartitionAssignment{Topic: "t1", Partition: 1},
					},
				},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 0},
						topicPartitionAssignment{Topic: "t1", Partition: 1},
					},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: []string{"c1"},
					topicPartitionAssignment{Topic: "t1", Partition: 1}: []string{"c1"},
				},
			},
			want: false,
		},
		{
			name: "Consumer has not been assigned all available partitions",
			args: args{
				memberID: "c1",
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 0},
						topicPartitionAssignment{Topic: "t1", Partition: 1},
					},
				},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 0},
						topicPartitionAssignment{Topic: "t1", Partition: 1},
						topicPartitionAssignment{Topic: "t1", Partition: 2},
					},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: []string{"c1"},
					topicPartitionAssignment{Topic: "t1", Partition: 1}: []string{"c1"},
					topicPartitionAssignment{Topic: "t1", Partition: 2}: []string{"c1"},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := canConsumerParticipateInReassignment(tt.args.memberID, tt.args.currentAssignment, tt.args.consumer2AllPotentialPartitions, tt.args.partition2AllPotentialConsumers); got != tt.want {
				t.Errorf("canConsumerParticipateInReassignment() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_removeTopicPartitionFromMemberAssignments(t *testing.T) {
	type args struct {
		assignments []topicPartitionAssignment
		topic       topicPartitionAssignment
	}
	tests := []struct {
		name string
		args args
		want []topicPartitionAssignment
	}{
		{
			name: "Empty",
			args: args{
				assignments: make([]topicPartitionAssignment, 0),
				topic:       topicPartitionAssignment{Topic: "t1", Partition: 0},
			},
			want: make([]topicPartitionAssignment, 0),
		},
		{
			name: "Remove first entry",
			args: args{
				assignments: []topicPartitionAssignment{
					topicPartitionAssignment{Topic: "t1", Partition: 0},
					topicPartitionAssignment{Topic: "t1", Partition: 1},
					topicPartitionAssignment{Topic: "t1", Partition: 2},
				},
				topic: topicPartitionAssignment{Topic: "t1", Partition: 0},
			},
			want: []topicPartitionAssignment{
				topicPartitionAssignment{Topic: "t1", Partition: 1},
				topicPartitionAssignment{Topic: "t1", Partition: 2},
			},
		},
		{
			name: "Remove middle entry",
			args: args{
				assignments: []topicPartitionAssignment{
					topicPartitionAssignment{Topic: "t1", Partition: 0},
					topicPartitionAssignment{Topic: "t1", Partition: 1},
					topicPartitionAssignment{Topic: "t1", Partition: 2},
				},
				topic: topicPartitionAssignment{Topic: "t1", Partition: 1},
			},
			want: []topicPartitionAssignment{
				topicPartitionAssignment{Topic: "t1", Partition: 0},
				topicPartitionAssignment{Topic: "t1", Partition: 2},
			},
		},
		{
			name: "Remove last entry",
			args: args{
				assignments: []topicPartitionAssignment{
					topicPartitionAssignment{Topic: "t1", Partition: 0},
					topicPartitionAssignment{Topic: "t1", Partition: 1},
					topicPartitionAssignment{Topic: "t1", Partition: 2},
				},
				topic: topicPartitionAssignment{Topic: "t1", Partition: 2},
			},
			want: []topicPartitionAssignment{
				topicPartitionAssignment{Topic: "t1", Partition: 0},
				topicPartitionAssignment{Topic: "t1", Partition: 1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := removeTopicPartitionFromMemberAssignments(tt.args.assignments, tt.args.topic); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("removeTopicPartitionFromMemberAssignments() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_removeIndexFromStringSlice(t *testing.T) {
	type args struct {
		s []string
		i int
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "Empty slice",
			args: args{
				s: make([]string, 0),
				i: 0,
			},
			want: make([]string, 0),
		},
		{
			name: "Slice with single entry",
			args: args{
				s: []string{"foo"},
				i: 0,
			},
			want: make([]string, 0),
		},
		{
			name: "Slice with multiple entries",
			args: args{
				s: []string{"a", "b", "c"},
				i: 0,
			},
			want: []string{"b", "c"},
		},
		{
			name: "Slice with multiple entries and index is in the middle",
			args: args{
				s: []string{"a", "b", "c"},
				i: 1,
			},
			want: []string{"a", "c"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := removeIndexFromStringSlice(tt.args.s, tt.args.i); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("removeIndexFromSlice() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_removeValueFromStringSlice(t *testing.T) {
	type args struct {
		s []string
		e string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "Empty input slice",
			args: args{
				s: []string{},
				e: "",
			},
			want: []string{},
		},
		{
			name: "Input slice with one entry that doesn't match",
			args: args{
				s: []string{"a"},
				e: "b",
			},
			want: []string{"a"},
		},
		{
			name: "Input slice with multiple entries and a positive match",
			args: args{
				s: []string{"a", "b", "c"},
				e: "b",
			},
			want: []string{"a", "c"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := removeValueFromStringSlice(tt.args.s, tt.args.e); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("removeValueFromSlice() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_assignPartition(t *testing.T) {
	type args struct {
		partition                       topicPartitionAssignment
		sortedCurrentSubscriptions      []string
		currentAssignment               map[string][]topicPartitionAssignment
		consumer2AllPotentialPartitions map[string][]topicPartitionAssignment
		currentPartitionConsumer        map[topicPartitionAssignment]string
	}
	tests := []struct {
		name                         string
		args                         args
		want                         []string
		wantCurrentAssignment        map[string][]topicPartitionAssignment
		wantCurrentPartitionConsumer map[topicPartitionAssignment]string
	}{
		{
			name: "Base",
			args: args{
				partition:                  topicPartitionAssignment{Topic: "t1", Partition: 2},
				sortedCurrentSubscriptions: []string{"c3", "c1", "c2"},
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 0},
					},
					"c2": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 1},
					},
					"c3": []topicPartitionAssignment{},
				},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 0},
					},
					"c2": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 1},
					},
					"c3": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 2},
					},
				},
				currentPartitionConsumer: map[topicPartitionAssignment]string{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: "c1",
					topicPartitionAssignment{Topic: "t1", Partition: 1}: "c2",
				},
			},
			want: []string{"c1", "c2", "c3"},
			wantCurrentAssignment: map[string][]topicPartitionAssignment{
				"c1": []topicPartitionAssignment{
					topicPartitionAssignment{Topic: "t1", Partition: 0},
				},
				"c2": []topicPartitionAssignment{
					topicPartitionAssignment{Topic: "t1", Partition: 1},
				},
				"c3": []topicPartitionAssignment{
					topicPartitionAssignment{Topic: "t1", Partition: 2},
				},
			},
			wantCurrentPartitionConsumer: map[topicPartitionAssignment]string{
				topicPartitionAssignment{Topic: "t1", Partition: 0}: "c1",
				topicPartitionAssignment{Topic: "t1", Partition: 1}: "c2",
				topicPartitionAssignment{Topic: "t1", Partition: 2}: "c3",
			},
		},
		{
			name: "Unassignable Partition",
			args: args{
				partition:                  topicPartitionAssignment{Topic: "t1", Partition: 3},
				sortedCurrentSubscriptions: []string{"c3", "c1", "c2"},
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 0},
					},
					"c2": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 1},
					},
					"c3": []topicPartitionAssignment{},
				},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 0},
					},
					"c2": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 1},
					},
					"c3": []topicPartitionAssignment{
						topicPartitionAssignment{Topic: "t1", Partition: 2},
					},
				},
				currentPartitionConsumer: map[topicPartitionAssignment]string{
					topicPartitionAssignment{Topic: "t1", Partition: 0}: "c1",
					topicPartitionAssignment{Topic: "t1", Partition: 1}: "c2",
				},
			},
			want: []string{"c3", "c1", "c2"},
			wantCurrentAssignment: map[string][]topicPartitionAssignment{
				"c1": []topicPartitionAssignment{
					topicPartitionAssignment{Topic: "t1", Partition: 0},
				},
				"c2": []topicPartitionAssignment{
					topicPartitionAssignment{Topic: "t1", Partition: 1},
				},
				"c3": []topicPartitionAssignment{},
			},
			wantCurrentPartitionConsumer: map[topicPartitionAssignment]string{
				topicPartitionAssignment{Topic: "t1", Partition: 0}: "c1",
				topicPartitionAssignment{Topic: "t1", Partition: 1}: "c2",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := assignPartition(tt.args.partition, tt.args.sortedCurrentSubscriptions, tt.args.currentAssignment, tt.args.consumer2AllPotentialPartitions, tt.args.currentPartitionConsumer); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("assignPartition() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(tt.args.currentAssignment, tt.wantCurrentAssignment) {
				t.Errorf("assignPartition() currentAssignment = %v, want %v", tt.args.currentAssignment, tt.wantCurrentAssignment)
			}
			if !reflect.DeepEqual(tt.args.currentPartitionConsumer, tt.wantCurrentPartitionConsumer) {
				t.Errorf("assignPartition() currentPartitionConsumer = %v, want %v", tt.args.currentPartitionConsumer, tt.wantCurrentPartitionConsumer)
			}
		})
	}
}

func Test_balanceStrategy_Plan(t *testing.T) {
	type fields struct {
		name   string
		coreFn func(plan BalanceStrategyPlan, memberIDs []string, topic string, partitions []int32)
	}
	type args struct {
		members map[string]ConsumerGroupMemberMetadata
		topics  map[string][]int32
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    BalanceStrategyPlan
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &balanceStrategy{
				name:   tt.fields.name,
				coreFn: tt.fields.coreFn,
			}
			got, err := s.Plan(tt.args.members, tt.args.topics)
			if (err != nil) != tt.wantErr {
				t.Errorf("balanceStrategy.Plan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("balanceStrategy.Plan() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_stickyBalanceStrategy_Plan(t *testing.T) {
	type args struct {
		members map[string]ConsumerGroupMemberMetadata
		topics  map[string][]int32
	}
	tests := []struct {
		name string
		s    *stickyBalanceStrategy
		args args
	}{
		{
			name: "One consumer with no topics",
			args: args{
				members: map[string]ConsumerGroupMemberMetadata{
					"consumer": ConsumerGroupMemberMetadata{},
				},
				topics: make(map[string][]int32),
			},
		},
		{
			name: "One consumer with non-existent topic",
			args: args{
				members: map[string]ConsumerGroupMemberMetadata{
					"consumer": ConsumerGroupMemberMetadata{
						Topics: []string{"topic"},
					},
				},
				topics: map[string][]int32{
					"topic": make([]int32, 0),
				},
			},
		},
		{
			name: "One consumer with one topic",
			args: args{
				members: map[string]ConsumerGroupMemberMetadata{
					"consumer": ConsumerGroupMemberMetadata{
						Topics: []string{"topic"},
					},
				},
				topics: map[string][]int32{
					"topic": []int32{0, 1, 2},
				},
			},
		},
		{
			name: "Only assigns partitions from subscribed topics",
			args: args{
				members: map[string]ConsumerGroupMemberMetadata{
					"consumer": ConsumerGroupMemberMetadata{
						Topics: []string{"topic"},
					},
				},
				topics: map[string][]int32{
					"topic": []int32{0, 1, 2},
					"other": []int32{0, 1, 2},
				},
			},
		},
		{
			name: "One consumer with multiple topics",
			args: args{
				members: map[string]ConsumerGroupMemberMetadata{
					"consumer": ConsumerGroupMemberMetadata{
						Topics: []string{"topic1", "topic2"},
					},
				},
				topics: map[string][]int32{
					"topic1": []int32{0},
					"topic2": []int32{0, 1},
				},
			},
		},
		{
			name: "Two consumers with one topic and one partition",
			args: args{
				members: map[string]ConsumerGroupMemberMetadata{
					"consumer1": ConsumerGroupMemberMetadata{
						Topics: []string{"topic"},
					},
					"consumer2": ConsumerGroupMemberMetadata{
						Topics: []string{"topic"},
					},
				},
				topics: map[string][]int32{
					"topic": []int32{0},
				},
			},
		},
		{
			name: "Two consumers with one topic and two partitions",
			args: args{
				members: map[string]ConsumerGroupMemberMetadata{
					"consumer1": ConsumerGroupMemberMetadata{
						Topics: []string{"topic"},
					},
					"consumer2": ConsumerGroupMemberMetadata{
						Topics: []string{"topic"},
					},
				},
				topics: map[string][]int32{
					"topic": []int32{0, 1},
				},
			},
		},
		{
			name: "Multiple consumers with mixed topic subscriptions",
			args: args{
				members: map[string]ConsumerGroupMemberMetadata{
					"consumer1": ConsumerGroupMemberMetadata{
						Topics: []string{"topic1"},
					},
					"consumer2": ConsumerGroupMemberMetadata{
						Topics: []string{"topic1", "topic2"},
					},
					"consumer3": ConsumerGroupMemberMetadata{
						Topics: []string{"topic1"},
					},
				},
				topics: map[string][]int32{
					"topic1": []int32{0, 1, 2},
					"topic2": []int32{0, 1},
				},
			},
		},
		{
			name: "Two consumers with two topics and six partitions",
			args: args{
				members: map[string]ConsumerGroupMemberMetadata{
					"consumer1": ConsumerGroupMemberMetadata{
						Topics: []string{"topic1", "topic2"},
					},
					"consumer2": ConsumerGroupMemberMetadata{
						Topics: []string{"topic1", "topic2"},
					},
				},
				topics: map[string][]int32{
					"topic1": []int32{0, 1, 2},
					"topic2": []int32{0, 1, 2},
				},
			},
		},
		{
			name: "Three consumers (two old, one new) with one topic and twelve partitions",
			args: args{
				members: map[string]ConsumerGroupMemberMetadata{
					"consumer1": ConsumerGroupMemberMetadata{
						Topics:   []string{"topic1"},
						UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": []int32{4, 11, 8, 5, 9, 2}}, 1),
					},
					"consumer2": ConsumerGroupMemberMetadata{
						Topics:   []string{"topic1"},
						UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": []int32{1, 3, 0, 7, 10, 6}}, 1),
					},
					"consumer3": ConsumerGroupMemberMetadata{
						Topics: []string{"topic1"},
					},
				},
				topics: map[string][]int32{
					"topic1": []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
				},
			},
		},
		{
			name: "Three consumers (two old, one new) with one topic and 13 partitions",
			args: args{
				members: map[string]ConsumerGroupMemberMetadata{
					"consumer1": ConsumerGroupMemberMetadata{
						Topics:   []string{"topic1"},
						UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": []int32{4, 11, 8, 5, 9, 2, 6}}, 1),
					},
					"consumer2": ConsumerGroupMemberMetadata{
						Topics:   []string{"topic1"},
						UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": []int32{1, 3, 0, 7, 10, 12}}, 1),
					},
					"consumer3": ConsumerGroupMemberMetadata{
						Topics: []string{"topic1"},
					},
				},
				topics: map[string][]int32{
					"topic1": []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &stickyBalanceStrategy{}
			plan, err := s.Plan(tt.args.members, tt.args.topics)
			verifyPlanIsBalancedAndSticky(t, s, tt.args.members, plan, err)
		})
	}
}

func Test_stickyBalanceStrategy_Plan_AddRemoveConsumerOneTopic(t *testing.T) {
	s := &stickyBalanceStrategy{}

	// PLAN 1
	members := map[string]ConsumerGroupMemberMetadata{
		"consumer1": ConsumerGroupMemberMetadata{
			Topics: []string{"topic"},
		},
	}
	topics := map[string][]int32{
		"topic": []int32{0, 1, 2},
	}
	plan1, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan1, err)

	// PLAN 2
	members["consumer1"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic"},
		UserData: encodeSubscriberPlan(t, plan1["consumer1"]),
	}
	members["consumer2"] = ConsumerGroupMemberMetadata{
		Topics: []string{"topic"},
	}
	plan2, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2, err)

	// PLAN 3
	delete(members, "consumer1")
	members["consumer2"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic"},
		UserData: encodeSubscriberPlan(t, plan2["consumer2"]),
	}
	plan3, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan3, err)
}

func Test_stickyBalanceStrategy_Plan_PoorRoundRobinAssignmentScenario(t *testing.T) {
	s := &stickyBalanceStrategy{}

	// PLAN 1
	members := map[string]ConsumerGroupMemberMetadata{
		"consumer1": ConsumerGroupMemberMetadata{
			Topics: []string{"topic1", "topic2", "topic3", "topic4", "topic5"},
		},
		"consumer2": ConsumerGroupMemberMetadata{
			Topics: []string{"topic1", "topic3", "topic5"},
		},
		"consumer3": ConsumerGroupMemberMetadata{
			Topics: []string{"topic1", "topic3", "topic5"},
		},
		"consumer4": ConsumerGroupMemberMetadata{
			Topics: []string{"topic1", "topic2", "topic3", "topic4", "topic5"},
		},
	}
	topics := make(map[string][]int32, 5)
	for i := 1; i <= 5; i++ {
		partitions := make([]int32, i%2+1)
		for j := 0; j < i%2+1; j++ {
			partitions[j] = int32(j)
		}
		topics[fmt.Sprintf("topic%d", i)] = partitions
	}

	plan, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan, err)
}

func Test_stickyBalanceStrategy_Plan_AddRemoveTopicTwoConsumers(t *testing.T) {
	s := &stickyBalanceStrategy{}

	// PLAN 1
	members := map[string]ConsumerGroupMemberMetadata{
		"consumer1": ConsumerGroupMemberMetadata{
			Topics: []string{"topic1"},
		},
		"consumer2": ConsumerGroupMemberMetadata{
			Topics: []string{"topic1"},
		},
	}
	topics := map[string][]int32{
		"topic1": []int32{0, 1, 2},
	}
	plan1, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan1, err)

	// PLAN 2
	members["consumer1"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1", "topic2"},
		UserData: encodeSubscriberPlan(t, plan1["consumer1"]),
	}
	members["consumer2"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1", "topic2"},
		UserData: encodeSubscriberPlan(t, plan1["consumer2"]),
	}
	topics["topic2"] = []int32{0, 1, 2}

	plan2, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2, err)

	// PLAN 3
	members["consumer1"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1", "topic2"},
		UserData: encodeSubscriberPlan(t, plan2["consumer1"]),
	}
	members["consumer2"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1", "topic2"},
		UserData: encodeSubscriberPlan(t, plan2["consumer2"]),
	}
	delete(topics, "topic1")

	plan3, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan3, err)
}

func Test_stickyBalanceStrategy_Plan_ReassignmentAfterOneConsumerLeaves(t *testing.T) {
	s := &stickyBalanceStrategy{}

	// PLAN 1
	members := make(map[string]ConsumerGroupMemberMetadata, 20)
	for i := 0; i < 20; i++ {
		topics := make([]string, 20)
		for j := 0; j < 20; j++ {
			topics[j] = fmt.Sprintf("topic%d", j)
		}
		members[fmt.Sprintf("consumer%d", i)] = ConsumerGroupMemberMetadata{Topics: topics}
	}
	topics := make(map[string][]int32, 20)
	for i := 0; i < 20; i++ {
		partitions := make([]int32, 20)
		for j := 0; j < 20; j++ {
			partitions[j] = int32(j)
		}
		topics[fmt.Sprintf("topic%d", i)] = partitions
	}

	plan, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan, err)

	for i := 0; i < 20; i++ {
		topics := make([]string, 20)
		for j := 0; j < 20; j++ {
			topics[j] = fmt.Sprintf("topic%d", j)
		}
		members[fmt.Sprintf("consumer%d", i)] = ConsumerGroupMemberMetadata{
			Topics:   members[fmt.Sprintf("consumer%d", i)].Topics,
			UserData: encodeSubscriberPlan(t, plan[fmt.Sprintf("consumer%d", i)]),
		}
	}
	delete(members, "consumer10")

	plan2, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2, err)
}

func Test_stickyBalanceStrategy_Plan_ReassignmentAfterOneConsumerAdded(t *testing.T) {
	s := &stickyBalanceStrategy{}

	// PLAN 1
	members := make(map[string]ConsumerGroupMemberMetadata)
	for i := 0; i < 10; i++ {
		members[fmt.Sprintf("consumer%d", i)] = ConsumerGroupMemberMetadata{Topics: []string{"topic1"}}
	}
	partitions := make([]int32, 20)
	for j := 0; j < 20; j++ {
		partitions[j] = int32(j)
	}
	topics := map[string][]int32{"topic1": partitions}

	plan, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan, err)

	// add a new consumer
	members["consumer10"] = ConsumerGroupMemberMetadata{Topics: []string{"topic1"}}

	plan2, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2, err)
}

func Test_stickyBalanceStrategy_Plan_SameSubscriptions(t *testing.T) {
	s := &stickyBalanceStrategy{}

	// PLAN 1
	members := make(map[string]ConsumerGroupMemberMetadata, 20)
	for i := 0; i < 9; i++ {
		topics := make([]string, 15)
		for j := 0; j < 15; j++ {
			topics[j] = fmt.Sprintf("topic%d", j)
		}
		members[fmt.Sprintf("consumer%d", i)] = ConsumerGroupMemberMetadata{Topics: topics}
	}
	topics := make(map[string][]int32, 15)
	for i := 0; i < 15; i++ {
		partitions := make([]int32, i)
		for j := 0; j < i; j++ {
			partitions[j] = int32(j)
		}
		topics[fmt.Sprintf("topic%d", i)] = partitions
	}

	plan, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan, err)

	// PLAN 2
	for i := 0; i < 9; i++ {
		members[fmt.Sprintf("consumer%d", i)] = ConsumerGroupMemberMetadata{
			Topics:   members[fmt.Sprintf("consumer%d", i)].Topics,
			UserData: encodeSubscriberPlan(t, plan[fmt.Sprintf("consumer%d", i)]),
		}
	}
	delete(members, "consumer5")

	plan2, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2, err)
}

func Test_stickyBalanceStrategy_Plan_LargeAssignmentWithMultipleConsumersLeaving(t *testing.T) {
	s := &stickyBalanceStrategy{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// PLAN 1
	members := make(map[string]ConsumerGroupMemberMetadata, 20)
	for i := 0; i < 200; i++ {
		topics := make([]string, 200)
		for j := 0; j < 200; j++ {
			topics[j] = fmt.Sprintf("topic%d", j)
		}
		members[fmt.Sprintf("consumer%d", i)] = ConsumerGroupMemberMetadata{Topics: topics}
	}
	topics := make(map[string][]int32, 40)
	for i := 0; i < 40; i++ {
		partitionCount := r.Intn(20)
		partitions := make([]int32, partitionCount)
		for j := 0; j < partitionCount; j++ {
			partitions[j] = int32(j)
		}
		topics[fmt.Sprintf("topic%d", i)] = partitions
	}

	plan, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan, err)

	for i := 0; i < 200; i++ {
		members[fmt.Sprintf("consumer%d", i)] = ConsumerGroupMemberMetadata{
			Topics:   members[fmt.Sprintf("consumer%d", i)].Topics,
			UserData: encodeSubscriberPlan(t, plan[fmt.Sprintf("consumer%d", i)]),
		}
	}
	for i := 0; i < 50; i++ {
		delete(members, fmt.Sprintf("consumer%d", i))
	}

	plan2, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2, err)
}

func Test_stickyBalanceStrategy_Plan_NewSubscription(t *testing.T) {
	s := &stickyBalanceStrategy{}

	members := make(map[string]ConsumerGroupMemberMetadata, 20)
	for i := 0; i < 3; i++ {
		topics := make([]string, 0)
		for j := i; j <= 3*i-2; j++ {
			topics = append(topics, fmt.Sprintf("topic%d", j))
		}
		members[fmt.Sprintf("consumer%d", i)] = ConsumerGroupMemberMetadata{Topics: topics}
	}
	topics := make(map[string][]int32, 5)
	for i := 1; i < 5; i++ {
		topics[fmt.Sprintf("topic%d", i)] = []int32{0}
	}

	plan, err := s.Plan(members, topics)
	if err != nil {
		t.Errorf("stickyBalanceStrategy.Plan() error = %v", err)
		return
	}
	verifyValidityAndBalance(t, members, plan)

	members["consumer0"] = ConsumerGroupMemberMetadata{Topics: []string{"topic1"}}

	plan2, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2, err)
}

func Test_stickyBalanceStrategy_Plan_ReassignmentWithRandomSubscriptionsAndChanges(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	minNumConsumers := 20
	maxNumConsumers := 40
	minNumTopics := 10
	maxNumTopics := 20

	for round := 0; round < 100; round++ {
		numTopics := minNumTopics + r.Intn(maxNumTopics-minNumTopics)
		topics := make([]string, numTopics)
		partitionsPerTopic := make(map[string][]int32, numTopics)
		for i := 0; i < numTopics; i++ {
			topicName := fmt.Sprintf("topic%d", i)
			topics[i] = topicName
			partitions := make([]int32, maxNumTopics)
			for j := 0; j < maxNumTopics; j++ {
				partitions[j] = int32(j)
			}
			partitionsPerTopic[topicName] = partitions
		}

		numConsumers := minNumConsumers + r.Intn(maxNumConsumers-minNumConsumers)
		members := make(map[string]ConsumerGroupMemberMetadata, numConsumers)
		for i := 0; i < numConsumers; i++ {
			sub := getRandomSublist(r, topics)
			sort.Strings(sub)
			members[fmt.Sprintf("consumer%d", i)] = ConsumerGroupMemberMetadata{Topics: sub}
		}

		s := &stickyBalanceStrategy{}
		plan, err := s.Plan(members, partitionsPerTopic)
		verifyPlanIsBalancedAndSticky(t, s, members, plan, err)

		// PLAN 2
		membersPlan2 := make(map[string]ConsumerGroupMemberMetadata, numConsumers)
		for i := 0; i < numConsumers; i++ {
			sub := getRandomSublist(r, topics)
			sort.Strings(sub)
			membersPlan2[fmt.Sprintf("consumer%d", i)] = ConsumerGroupMemberMetadata{
				Topics:   sub,
				UserData: encodeSubscriberPlan(t, plan[fmt.Sprintf("consumer%d", i)]),
			}
		}
		plan2, err := s.Plan(membersPlan2, partitionsPerTopic)
		verifyPlanIsBalancedAndSticky(t, s, members, plan2, err)
	}
}

func Test_stickyBalanceStrategy_Plan_MoveExistingAssignments(t *testing.T) {
	s := &stickyBalanceStrategy{}

	topics := make(map[string][]int32, 6)
	for i := 1; i <= 6; i++ {
		topics[fmt.Sprintf("topic%d", i)] = []int32{0}
	}
	members := make(map[string]ConsumerGroupMemberMetadata, 3)
	members["consumer1"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1", "topic2"},
		UserData: encodeSubscriberPlan(t, map[string][]int32{"topic1": []int32{0}}),
	}
	members["consumer2"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1", "topic2", "topic3", "topic4"},
		UserData: encodeSubscriberPlan(t, map[string][]int32{"topic2": []int32{0}, "topic3": []int32{0}}),
	}
	members["consumer3"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic2", "topic3", "topic4", "topic5", "topic6"},
		UserData: encodeSubscriberPlan(t, map[string][]int32{"topic4": []int32{0}, "topic5": []int32{0}, "topic6": []int32{0}}),
	}

	plan, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan, err)
}

func Test_stickyBalanceStrategy_Plan_Stickiness(t *testing.T) {
	s := &stickyBalanceStrategy{}

	topics := map[string][]int32{"topic1": []int32{0, 1, 2}}
	members := map[string]ConsumerGroupMemberMetadata{
		"consumer1": ConsumerGroupMemberMetadata{Topics: []string{"topic1"}},
		"consumer2": ConsumerGroupMemberMetadata{Topics: []string{"topic1"}},
		"consumer3": ConsumerGroupMemberMetadata{Topics: []string{"topic1"}},
		"consumer4": ConsumerGroupMemberMetadata{Topics: []string{"topic1"}},
	}

	plan, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan, err)

	// PLAN 2
	// remove the potential group leader
	delete(members, "consumer1")
	for i := 2; i <= 4; i++ {
		members[fmt.Sprintf("consumer%d", i)] = ConsumerGroupMemberMetadata{
			Topics:   []string{"topic1"},
			UserData: encodeSubscriberPlan(t, plan[fmt.Sprintf("consumer%d", i)]),
		}
	}

	plan2, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2, err)
}

func Test_stickyBalanceStrategy_Plan_AssignmentUpdatedForDeletedTopic(t *testing.T) {
	s := &stickyBalanceStrategy{}

	topics := make(map[string][]int32, 2)
	topics["topic1"] = []int32{0}
	topics["topic3"] = make([]int32, 100)
	for i := 0; i < 100; i++ {
		topics["topic3"][i] = int32(i)
	}
	members := map[string]ConsumerGroupMemberMetadata{
		"consumer1": ConsumerGroupMemberMetadata{Topics: []string{"topic1", "topic2", "topic3"}},
	}

	plan, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan, err)
	if (len(plan["consumer1"]["topic1"]) + len(plan["consumer1"]["topic3"])) != 101 {
		t.Error("Incorrect number of partitions assigned")
		return
	}
}

func Test_stickyBalanceStrategy_Plan_NoExceptionRaisedWhenOnlySubscribedTopicDeleted(t *testing.T) {
	s := &stickyBalanceStrategy{}

	topics := map[string][]int32{"topic1": []int32{0, 1, 2}}
	members := map[string]ConsumerGroupMemberMetadata{
		"consumer1": ConsumerGroupMemberMetadata{Topics: []string{"topic1"}},
	}
	plan, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan, err)

	// PLAN 2
	members["consumer1"] = ConsumerGroupMemberMetadata{
		Topics:   members["consumer1"].Topics,
		UserData: encodeSubscriberPlan(t, plan["consumer1"]),
	}

	plan2, err := s.Plan(members, map[string][]int32{})
	if len(plan2) != 1 {
		t.Error("Incorrect number of consumers")
		return
	}
	if len(plan2["consumer1"]) != 0 {
		t.Error("Incorrect number of consumer topic assignments")
		return
	}
	verifyPlanIsBalancedAndSticky(t, s, members, plan2, err)
}

func Test_stickyBalanceStrategy_Plan_AssignmentWithMultipleGenerations1(t *testing.T) {
	s := &stickyBalanceStrategy{}

	topics := map[string][]int32{"topic1": []int32{0, 1, 2, 3, 4, 5}}
	members := map[string]ConsumerGroupMemberMetadata{
		"consumer1": ConsumerGroupMemberMetadata{Topics: []string{"topic1"}},
		"consumer2": ConsumerGroupMemberMetadata{Topics: []string{"topic1"}},
		"consumer3": ConsumerGroupMemberMetadata{Topics: []string{"topic1"}},
	}
	plan, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan, err)

	// PLAN 2
	members["consumer1"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1"},
		UserData: encodeSubscriberPlanWithGeneration(t, plan["consumer1"], 1),
	}
	members["consumer2"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1"},
		UserData: encodeSubscriberPlanWithGeneration(t, plan["consumer2"], 1),
	}
	delete(members, "consumer3")

	plan2, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2, err)
	if len(intersection(plan["consumer1"]["topic1"], plan2["consumer1"]["topic1"])) != 2 {
		t.Error("stickyBalanceStrategy.Plan() consumer1 didn't maintain partitions across reassignment")
	}
	if len(intersection(plan["consumer2"]["topic1"], plan2["consumer2"]["topic1"])) != 2 {
		t.Error("stickyBalanceStrategy.Plan() consumer1 didn't maintain partitions across reassignment")
	}

	// PLAN 3
	delete(members, "consumer1")
	members["consumer2"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1"},
		UserData: encodeSubscriberPlanWithGeneration(t, plan2["consumer2"], 2),
	}
	members["consumer3"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1"},
		UserData: encodeSubscriberPlanWithGeneration(t, plan["consumer3"], 1),
	}

	plan3, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan3, err)
}

func Test_stickyBalanceStrategy_Plan_AssignmentWithMultipleGenerations2(t *testing.T) {
	s := &stickyBalanceStrategy{}

	topics := map[string][]int32{"topic1": []int32{0, 1, 2, 3, 4, 5}}
	members := map[string]ConsumerGroupMemberMetadata{
		"consumer1": ConsumerGroupMemberMetadata{Topics: []string{"topic1"}},
		"consumer2": ConsumerGroupMemberMetadata{Topics: []string{"topic1"}},
		"consumer3": ConsumerGroupMemberMetadata{Topics: []string{"topic1"}},
	}
	plan, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan, err)

	// PLAN 2
	delete(members, "consumer1")
	members["consumer2"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1"},
		UserData: encodeSubscriberPlanWithGeneration(t, plan["consumer2"], 1),
	}
	delete(members, "consumer3")

	plan2, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2, err)
	if len(intersection(plan["consumer2"]["topic1"], plan2["consumer2"]["topic1"])) != 2 {
		t.Error("stickyBalanceStrategy.Plan() consumer1 didn't maintain partitions across reassignment")
	}

	// PLAN 3
	members["consumer1"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1"},
		UserData: encodeSubscriberPlanWithGeneration(t, plan["consumer1"], 1),
	}
	members["consumer2"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1"},
		UserData: encodeSubscriberPlanWithGeneration(t, plan2["consumer2"], 2),
	}
	members["consumer3"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1"},
		UserData: encodeSubscriberPlanWithGeneration(t, plan["consumer3"], 1),
	}
	plan3, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan3, err)
}
func Test_stickyBalanceStrategy_Plan_AssignmentWithConflictingPreviousGenerations(t *testing.T) {
	s := &stickyBalanceStrategy{}

	topics := map[string][]int32{"topic1": []int32{0, 1, 2, 3, 4, 5}}
	members := make(map[string]ConsumerGroupMemberMetadata, 3)
	members["consumer1"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1"},
		UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": []int32{0, 1, 4}}, 1),
	}
	members["consumer2"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1"},
		UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": []int32{0, 2, 3}}, 1),
	}
	members["consumer3"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1"},
		UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": []int32{3, 4, 5}}, 2),
	}

	plan, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan, err)
}

func Test_stickyBalanceStrategy_Plan_SchemaBackwardCompatibility(t *testing.T) {
	s := &stickyBalanceStrategy{}

	topics := map[string][]int32{"topic1": []int32{0, 1, 2}}
	members := make(map[string]ConsumerGroupMemberMetadata, 3)
	members["consumer1"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1"},
		UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": []int32{0, 2}}, 1),
	}
	members["consumer2"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1"},
		UserData: encodeSubscriberPlanWithOldSchema(t, map[string][]int32{"topic1": []int32{1}}),
	}
	members["consumer3"] = ConsumerGroupMemberMetadata{Topics: []string{"topic1"}}

	plan, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan, err)
}

func Test_stickyBalanceStrategy_Plan_ConflictingPreviousAssignments(t *testing.T) {
	s := &stickyBalanceStrategy{}

	topics := map[string][]int32{"topic1": []int32{0, 1}}
	members := make(map[string]ConsumerGroupMemberMetadata, 2)
	members["consumer1"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1"},
		UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": []int32{0, 1}}, 1),
	}
	members["consumer2"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic1"},
		UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": []int32{0, 1}}, 1),
	}

	plan, err := s.Plan(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan, err)
}

func verifyPlanIsBalancedAndSticky(t *testing.T, s *stickyBalanceStrategy, members map[string]ConsumerGroupMemberMetadata, plan BalanceStrategyPlan, err error) {
	if err != nil {
		t.Errorf("stickyBalanceStrategy.Plan() error = %v", err)
		return
	}
	if !isFullyBalanced(plan) {
		t.Error("stickyBalanceStrategy.Plan() unbalanced")
		return
	}
	if !s.movements.isSticky() {
		t.Error("stickyBalanceStrategy.Plan() not sticky")
		return
	}
	verifyValidityAndBalance(t, members, plan)
}

func verifyValidityAndBalance(t *testing.T, consumers map[string]ConsumerGroupMemberMetadata, plan BalanceStrategyPlan) {
	size := len(consumers)
	if size != len(plan) {
		t.Errorf("Subscription size (%d) not equal to plan size (%d)", size, len(plan))
		t.FailNow()
	}

	members := make([]string, size)
	i := 0
	for memberID := range consumers {
		members[i] = memberID
		i++
	}
	sort.Strings(members)

	for i, memberID := range members {
		for assignedTopic := range plan[memberID] {
			found := false
			for _, assignableTopic := range consumers[memberID].Topics {
				if assignableTopic == assignableTopic {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Consumer %s had assigned topic %s that wasn't in the list of assignable topics", memberID, assignedTopic)
				t.FailNow()
			}
		}

		// skip last consumer
		if i == len(members)-1 {
			continue
		}

		consumerAssignments := make([]topicPartitionAssignment, 0)
		for topic, partitions := range plan[memberID] {
			for _, partition := range partitions {
				consumerAssignments = append(consumerAssignments, topicPartitionAssignment{Topic: topic, Partition: partition})
			}
		}

		for j := i + 1; j < size; j++ {
			otherConsumer := members[j]
			otherConsumerAssignments := make([]topicPartitionAssignment, 0)
			for topic, partitions := range plan[otherConsumer] {
				for _, partition := range partitions {
					otherConsumerAssignments = append(otherConsumerAssignments, topicPartitionAssignment{Topic: topic, Partition: partition})
				}
			}
			assignmentsIntersection := intersection(consumerAssignments, otherConsumerAssignments)
			if len(assignmentsIntersection) > 0 {
				t.Errorf("Consumers %s and %s have common partitions assigned to them: %v", memberID, otherConsumer, assignmentsIntersection)
				t.FailNow()
			}

			if math.Abs(float64(len(consumerAssignments)-len(otherConsumerAssignments))) <= 1 {
				continue
			}

			if len(consumerAssignments) > len(otherConsumerAssignments) {
				for _, topic := range consumerAssignments {
					if _, exists := plan[otherConsumer][topic.Topic]; exists {
						t.Errorf("Some partitions can be moved from %s to %s to achieve a better balance, %s has %d assignments, and %s has %d assignments", otherConsumer, memberID, memberID, len(consumerAssignments), otherConsumer, len(otherConsumerAssignments))
						t.FailNow()
					}
				}
			}

			if len(otherConsumerAssignments) > len(consumerAssignments) {
				for _, topic := range otherConsumerAssignments {
					if _, exists := plan[memberID][topic.Topic]; exists {
						t.Errorf("Some partitions can be moved from %s to %s to achieve a better balance, %s has %d assignments, and %s has %d assignments", memberID, otherConsumer, otherConsumer, len(otherConsumerAssignments), memberID, len(consumerAssignments))
						t.FailNow()
					}
				}
			}
		}
	}
}

// Produces the intersection of two slices
// From https://github.com/juliangruber/go-intersect
func intersection(a interface{}, b interface{}) []interface{} {
	set := make([]interface{}, 0)
	hash := make(map[interface{}]bool)
	av := reflect.ValueOf(a)
	bv := reflect.ValueOf(b)

	for i := 0; i < av.Len(); i++ {
		el := av.Index(i).Interface()
		hash[el] = true
	}

	for i := 0; i < bv.Len(); i++ {
		el := bv.Index(i).Interface()
		if _, found := hash[el]; found {
			set = append(set, el)
		}
	}

	return set
}

func encodeSubscriberPlan(t *testing.T, assignments map[string][]int32) []byte {
	return encodeSubscriberPlanWithGeneration(t, assignments, defaultGeneration)
}

func encodeSubscriberPlanWithGeneration(t *testing.T, assignments map[string][]int32, generation int32) []byte {
	userDataBytes, err := encode(&StickyAssignorUserDataV1{
		Topics:     assignments,
		Generation: generation,
	}, nil)
	if err != nil {
		t.Errorf("encodeSubscriberPlan error = %v", err)
		t.FailNow()
	}
	return userDataBytes
}

func encodeSubscriberPlanWithOldSchema(t *testing.T, assignments map[string][]int32) []byte {
	userDataBytes, err := encode(&StickyAssignorUserDataV0{
		Topics: assignments,
	}, nil)
	if err != nil {
		t.Errorf("encodeSubscriberPlan error = %v", err)
		t.FailNow()
	}
	return userDataBytes
}

// verify that the plan is fully balanced, assumes that all consumers can
// consume from the same set of topics
func isFullyBalanced(plan BalanceStrategyPlan) bool {
	min := math.MaxInt32
	max := math.MinInt32
	for _, topics := range plan {
		assignedPartitionsCount := 0
		for _, partitions := range topics {
			assignedPartitionsCount += len(partitions)
		}
		if assignedPartitionsCount < min {
			min = assignedPartitionsCount
		}
		if assignedPartitionsCount > max {
			max = assignedPartitionsCount
		}
	}
	return (max - min) <= 1
}

func getRandomSublist(r *rand.Rand, s []string) []string {
	howManyToRemove := r.Intn(len(s))
	allEntriesMap := make(map[int]string)
	for i, s := range s {
		allEntriesMap[i] = s
	}
	for i := 0; i < howManyToRemove; i++ {
		delete(allEntriesMap, r.Intn(len(allEntriesMap)))
	}

	subList := make([]string, len(allEntriesMap))
	i := 0
	for _, s := range allEntriesMap {
		subList[i] = s
		i++
	}
	return subList
}
