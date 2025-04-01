package planner

import (
	"slices"

	"github.com/draganm/datas3t/pkg/client"
)

type AggregationOperation []client.DataRange

func (p AggregationOperation) SizeBytes() uint64 {
	size := uint64(0)
	for _, dr := range p {
		size += dr.SizeBytes
	}
	return size
}

func (p AggregationOperation) StartKey() uint64 {
	return p[0].MinDatapointKey
}

func (p AggregationOperation) EndKey() uint64 {
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
	10 * 1024 * 1024,         // 10MB
	1 * 1024 * 1024 * 1024,   // 1GB
	100 * 1024 * 1024 * 1024, // 100GB
}

var topLevel = len(levelTresholds)

func CreatePlan(dataranges []client.DataRange) []AggregationOperation {
	if len(dataranges) < 2 {
		return []AggregationOperation{}
	}
	slices.SortFunc(dataranges, func(a, b client.DataRange) int {
		return int(a.MinDatapointKey - b.MaxDatapointKey)
	})

	prevLevel := topLevel

	plan := []AggregationOperation{}

	for len(dataranges) > 0 {
		// Find consecutive ranges

		prev := dataranges[0]
		aggregation := AggregationOperation{prev}

		for _, dr := range dataranges[1:] {
			if prev.MaxDatapointKey+1 != dr.MinDatapointKey {
				break
			}
			aggregation = append(aggregation, dr)
			prev = dr
		}

		if len(aggregation) == 1 {
			dataranges = dataranges[1:]
			continue
		}

		for aggregation.Level() >= prevLevel && len(aggregation) > 1 {
			aggregation = aggregation[:len(aggregation)-1]
		}

		if len(aggregation) == 1 {
			dataranges = dataranges[1:]
			continue
		}

		if aggregation.Level() > DatarangeLevel(aggregation[0]) {
			plan = append(plan, aggregation)
			dataranges = dataranges[len(aggregation):]
			prevLevel = aggregation.Level()
			continue
		}

		// edge case: if datapoints are small, we can't rely on datasets size
		if len(aggregation) >= 1000 {
			numberOfDatasets := float64(len(aggregation))
			numberOfDatapoints := float64(aggregation.NumberOfDatapoints())
			datapointsPerDataset := numberOfDatapoints / numberOfDatasets

			if datapointsPerDataset < 10 {
				plan = append(plan, aggregation)
				dataranges = dataranges[len(aggregation):]
				prevLevel = aggregation.Level()
				continue
			}
		}

		dataranges = dataranges[1:]
	}

	return plan
}
