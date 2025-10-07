package mounttos

import (
	"net/url"
	"sort"

	"github.com/GBA-BI/tes-k8s-agent/pkg/log"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient/models"
)

func extractBucketsAndSort(inputs []*models.Input) []string {
	bucketCounts := make(map[string]int)
	for _, input := range inputs {
		bucket := extractBucket(input.URL)
		if bucket == "" {
			continue
		}
		bucketCounts[bucket]++
	}
	res := make([]string, 0, len(bucketCounts))
	for bucket := range bucketCounts {
		res = append(res, bucket)
	}
	sort.Slice(res, func(i, j int) bool {
		return bucketCounts[res[i]] > bucketCounts[res[j]]
	})
	return res
}

func extractBucket(taskURL string) string {
	u, err := url.Parse(taskURL)
	if err != nil {
		log.Errorw("failed to parse task url", "err", err)
		return ""
	}
	if u.Scheme != consts.S3Type {
		return ""
	}
	return u.Host
}
