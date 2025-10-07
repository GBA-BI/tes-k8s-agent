package utils

import ctrl "sigs.k8s.io/controller-runtime"

// MergeCtrlResults ...
func MergeCtrlResults(results ...ctrl.Result) ctrl.Result {
	res := ctrl.Result{}
	for _, r := range results {
		if r.Requeue {
			res.Requeue = true
		}
		if r.RequeueAfter > 0 {
			if res.RequeueAfter == 0 || r.RequeueAfter < res.RequeueAfter {
				res.RequeueAfter = r.RequeueAfter
			}
		}
	}
	return res
}
