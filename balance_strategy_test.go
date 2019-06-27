package sarama

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"
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
			// userData := &StickyAssignorUserDataV1{}
			// decode(tt.args.members["c01"].UserData, userData)
			// userData.Generation = 5
			// packet, err := encode(userData, nil)
			// fmt.Printf("Gen 5: %s", base64.StdEncoding.EncodeToString(packet))

			// userData.Generation = 6
			// packet, err = encode(userData, nil)
			// fmt.Printf("Gen 6: %s", base64.StdEncoding.EncodeToString(packet))

			// t.FailNow()

			_, gotPrevAssignments, err := prepopulateCurrentAssignments(tt.args.members)

			if (err != nil) != tt.wantErr {
				t.Errorf("prepopulateCurrentAssignments() error = %v, wantErr %v", err, tt.wantErr)
			}

			// if !reflect.DeepEqual(gotCurrentAssignments, tt.wantCurrentAssignments) {
			// 	t.Errorf("deserializeTopicPartitionAssignment() currentAssignments = %v, want %v", gotCurrentAssignments, tt.wantCurrentAssignments)
			// }

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

func Test_removeIndexFromSlice(t *testing.T) {
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
			if got := removeIndexFromSlice(tt.args.s, tt.args.i); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("removeIndexFromSlice() = %v, want %v", got, tt.want)
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

func Test_removeValueFromSlice(t *testing.T) {
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
			if got := removeValueFromSlice(tt.args.s, tt.args.e); !reflect.DeepEqual(got, tt.want) {
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
		name    string
		s       *stickyBalanceStrategy
		args    args
		want    BalanceStrategyPlan
		wantErr bool
	}{
		{
			name: "One consumer with no topics",
			args: args{
				members: map[string]ConsumerGroupMemberMetadata{
					"consumer": ConsumerGroupMemberMetadata{},
				},
				topics: make(map[string][]int32),
			},
			want:    BalanceStrategyPlan{},
			wantErr: false,
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
			want:    BalanceStrategyPlan{},
			wantErr: false,
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
			want: BalanceStrategyPlan{
				"consumer": map[string][]int32{
					"topic": []int32{0, 1, 2},
				},
			},
			wantErr: false,
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
			want: BalanceStrategyPlan{
				"consumer": map[string][]int32{
					"topic": []int32{0, 1, 2},
				},
			},
			wantErr: false,
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
			want: BalanceStrategyPlan{
				"consumer": map[string][]int32{
					"topic1": []int32{0},
					"topic2": []int32{0, 1},
				},
			},
			wantErr: false,
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
			want: BalanceStrategyPlan{
				"consumer1": map[string][]int32{
					"topic": []int32{0},
				},
			},
			wantErr: false,
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
			wantErr: false,
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
			wantErr: false,
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
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &stickyBalanceStrategy{}
			got, err := s.Plan(tt.args.members, tt.args.topics)
			if (err != nil) != tt.wantErr {
				t.Errorf("stickyBalanceStrategy.Plan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !isFullyBalanced(got) {
				t.Error("stickyBalanceStrategy.Plan() unbalanced")
				return
			}
			verifyValidityAndBalance(t, tt.args.members, got)
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
	if err != nil {
		t.Errorf("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic error = %v", err)
		return
	}
	if !isFullyBalanced(plan1) {
		t.Error("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic unbalanced")
		return
	}
	verifyValidityAndBalance(t, members, plan1)

	// PLAN 2
	members["consumer1"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic"},
		UserData: encodeSubscriberPlan(t, plan1["consumer1"]),
	}
	members["consumer2"] = ConsumerGroupMemberMetadata{
		Topics: []string{"topic"},
	}
	plan2, err := s.Plan(members, topics)
	if err != nil {
		t.Errorf("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic plan 2 error = %v", err)
		return
	}
	if !isFullyBalanced(plan2) {
		t.Error("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic plan 2 is unbalanced")
		return
	}
	if !s.movements.isSticky() {
		t.Error("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic plan 2 not sticky")
		return
	}
	verifyValidityAndBalance(t, members, plan2)

	// PLAN 3
	delete(members, "consumer1")
	members["consumer2"] = ConsumerGroupMemberMetadata{
		Topics:   []string{"topic"},
		UserData: encodeSubscriberPlan(t, plan2["consumer2"]),
	}
	plan3, err := s.Plan(members, topics)
	if err != nil {
		t.Errorf("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic plan 3 error = %v", err)
		return
	}
	if !isFullyBalanced(plan3) {
		t.Error("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic plan 3 is unbalanced")
		return
	}
	if !s.movements.isSticky() {
		t.Error("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic plan 3 not sticky")
		return
	}
	verifyValidityAndBalance(t, members, plan3)
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
	if err != nil {
		t.Errorf("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic error = %v", err)
		return
	}
	if !isFullyBalanced(plan) {
		t.Error("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic unbalanced")
		return
	}
	verifyValidityAndBalance(t, members, plan)
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
	if err != nil {
		t.Errorf("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic error = %v", err)
		return
	}
	if !isFullyBalanced(plan1) {
		t.Error("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic unbalanced")
		return
	}
	verifyValidityAndBalance(t, members, plan1)

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
	if err != nil {
		t.Errorf("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic plan 2 error = %v", err)
		return
	}
	if !isFullyBalanced(plan2) {
		t.Error("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic plan 2 is unbalanced")
		return
	}
	if !s.movements.isSticky() {
		t.Error("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic plan 2 not sticky")
		return
	}
	verifyValidityAndBalance(t, members, plan2)

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
	if err != nil {
		t.Errorf("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic plan 3 error = %v", err)
		return
	}
	if !isFullyBalanced(plan3) {
		t.Error("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic plan 3 is unbalanced")
		return
	}
	if !s.movements.isSticky() {
		t.Error("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic plan 3 not sticky")
		return
	}
	verifyValidityAndBalance(t, members, plan3)
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
	if err != nil {
		t.Errorf("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic error = %v", err)
		return
	}
	if !isFullyBalanced(plan) {
		t.Error("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic unbalanced")
		return
	}
	verifyValidityAndBalance(t, members, plan)

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
	if err != nil {
		t.Errorf("stickyBalanceStrategy.Plan() plan 2 AddRemoveConsumerOneTopic error = %v", err)
		return
	}
	if !isFullyBalanced(plan2) {
		t.Error("stickyBalanceStrategy.Plan() plan 2 AddRemoveConsumerOneTopic unbalanced")
		return
	}
	if !s.movements.isSticky() {
		t.Error("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic plan 2 not sticky")
		return
	}
	verifyValidityAndBalance(t, members, plan2)
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
	if err != nil {
		t.Errorf("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic error = %v", err)
		return
	}
	if !isFullyBalanced(plan) {
		t.Error("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic unbalanced")
		return
	}
	verifyValidityAndBalance(t, members, plan)

	// add a new consumer
	members["consumer10"] = ConsumerGroupMemberMetadata{Topics: []string{"topic1"}}

	plan2, err := s.Plan(members, topics)
	if err != nil {
		t.Errorf("stickyBalanceStrategy.Plan() plan 2 AddRemoveConsumerOneTopic error = %v", err)
		return
	}
	if !isFullyBalanced(plan2) {
		t.Error("stickyBalanceStrategy.Plan() plan 2 AddRemoveConsumerOneTopic unbalanced")
		return
	}
	if !s.movements.isSticky() {
		t.Error("stickyBalanceStrategy.Plan() AddRemoveConsumerOneTopic plan 2 not sticky")
		return
	}
	verifyValidityAndBalance(t, members, plan2)
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
			intersection := Hash(consumerAssignments, otherConsumerAssignments)
			if len(intersection) > 0 {
				t.Errorf("Consumers %s nad %s have common partitions assigned to them: %v", memberID, otherConsumer, intersection)
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
func Hash(a interface{}, b interface{}) []interface{} {
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
	userDataBytes, err := encode(&StickyAssignorUserDataV1{
		Topics:     assignments,
		Generation: defaultGeneration,
	}, nil)
	if err != nil {
		t.Errorf("encodeSubscriberPlan error = %v", err)
		t.FailNow()
	}
	return userDataBytes
}

// verify that the plan is fully balanced
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
