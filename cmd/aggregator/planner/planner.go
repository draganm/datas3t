package planner

import (
	"fmt"

	"github.com/draganm/datas3t/pkg/client"
)

type AggregationOperation []client.DataRange

func (p AggregationOperation) SizeBytes() uint64 {
	size := uint64(0)
	for _, dr := range p {
		size += dr.SizeBytes - 1024
	}
	return size + 1024
}

func (p AggregationOperation) FromKey() uint64 {
	return p[0].MinDatapointKey
}

func (p AggregationOperation) ToKey() uint64 {
	return p[len(p)-1].MaxDatapointKey
}

func (p AggregationOperation) Level() int {
	for i, levelTreshold := range levelTresholds {
		if p.SizeBytes() < levelTreshold {
			return i
		}
	}
	return topLevel
}

func (p AggregationOperation) NumberOfDatapoints() uint64 {
	num := uint64(0)
	for _, dr := range p {
		num += dr.MaxDatapointKey - dr.MinDatapointKey + 1
	}
	return num
}

func DatarangeLevel(dr client.DataRange) int {
	for i, levelTreshold := range levelTresholds {
		if dr.SizeBytes < levelTreshold {
			return i
		}
	}
	return topLevel
}

var levelTresholds = []uint64{
	10 * 1024 * 1024,       // 10MB
	100 * 1024 * 1024,      // 100MB
	1 * 1024 * 1024 * 1024, // 1GB
}

var topLevel = len(levelTresholds)

type ContinuousRange struct {
	FromDatapointKey uint64
	ToDatapointKey   uint64
}

type AggregationOperationPlan []ContinuousRange

func CreatePlan(dataranges []client.DataRange) (AggregationOperationPlan, error) {
	plan := AggregationOperationPlan{}

	for len(dataranges) > 0 {
		toCompact := AggregationOperation{dataranges[0]}
		// step 1: find the first continuous range

		for _, dr := range dataranges[1:] {

			if dr.MinDatapointKey == toCompact[len(toCompact)-1].MaxDatapointKey+1 {
				toCompact = append(toCompact, dr)
				continue
			}

			if dr.MinDatapointKey > toCompact[len(toCompact)-1].MaxDatapointKey+1 {
				break
			}

			// TODO proper error
			return nil, fmt.Errorf("overlapping dataranges")
		}

		if !shouldCompact(toCompact) {
			dataranges = dataranges[1:]
			continue
		}

		plan = append(plan, ContinuousRange{
			FromDatapointKey: toCompact.FromKey(),
			ToDatapointKey:   toCompact.ToKey(),
		})

		dataranges = dataranges[len(toCompact):]

	}

	return plan, nil

}

func shouldCompact(dataranges AggregationOperation) bool {
	if len(dataranges) == 1 {
		return false
	}

	firstLevel := DatarangeLevel(dataranges[0])
	newLevel := dataranges.Level()

	if newLevel > firstLevel {
		return true
	}

	numberOfDatapoints := dataranges.NumberOfDatapoints()
	numberOfDatasets := float64(len(dataranges))
	datapointsPerDataset := float64(numberOfDatapoints) / numberOfDatasets

	return datapointsPerDataset < 10.0
}
