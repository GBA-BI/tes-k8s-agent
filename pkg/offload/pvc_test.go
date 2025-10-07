package offload

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
)

var inputsJSON = []byte(`{"inputs":[{"name":"input","path":"/base/xxx.txt","url":"s3://abcd.com/bbb/xxx.txt","type":"FILE"}}]}`)
var outputsJSON = []byte(`{"outputs":[{"name":"output","path":"/base/xxx.txt","url":"s3://abcd.com/bbb/xxx.txt","type":"FILE"}}]}`)

func TestOffloadInputs(t *testing.T) {
	g := gomega.NewWithT(t)

	path, err := os.MkdirTemp("", "test-offload")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(path)

	h := &pvcHelper{path: path}
	filePath, err := h.OffloadInputs("task-xxxx", inputsJSON)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(filePath).To(gomega.Equal(filepath.Join(path, "task-xxxx", inputsFileName)))
	content, err := os.ReadFile(filePath)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(content).To(gomega.Equal(inputsJSON))
}

func TestOffloadOutputs(t *testing.T) {
	g := gomega.NewWithT(t)

	path, err := os.MkdirTemp("", "test-offload")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(path)

	h := &pvcHelper{path: path}
	filePath, err := h.OffloadOutputs("task-xxxx", outputsJSON)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(filePath).To(gomega.Equal(filepath.Join(path, "task-xxxx", outputsFileName)))
	content, err := os.ReadFile(filePath)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(content).To(gomega.Equal(outputsJSON))
}

func TestDeleteOffloadFile(t *testing.T) {
	g := gomega.NewWithT(t)

	path, err := os.MkdirTemp("", "test-offload")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(path)

	h := &pvcHelper{path: path}
	_, err = h.OffloadInputs("task-xxxx", inputsJSON)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	_, err = h.OffloadOutputs("task-xxxx", outputsJSON)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	h.DeleteOffloadFile("task-xxxx")
	fileList, err := os.ReadDir(path)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(fileList).To(gomega.BeEmpty())
}

func TestModifyInputsFiler(t *testing.T) {
	g := gomega.NewWithT(t)

	podTemplate := &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "task-xxxx",
				VolumeMounts: []corev1.VolumeMount{{
					Name: "other",
				}},
				Env: []corev1.EnvVar{{
					Name:  "OTHER",
					Value: "value",
				}},
			}},
			Volumes: []corev1.Volume{{
				Name: "other",
			}},
		},
	}

	h := &pvcHelper{path: "/offload", pvcName: "offload-pvc"}
	h.ModifyInputsFiler("task-xxxx", podTemplate)
	g.Expect(podTemplate).To(gomega.BeEquivalentTo(&corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "task-xxxx",
				VolumeMounts: []corev1.VolumeMount{{
					Name: "other",
				}, {
					Name:      offloadVolumeName,
					MountPath: filepath.Join(h.path, "task-xxxx"),
					SubPath:   "task-xxxx",
					ReadOnly:  true,
				}},
				Env: []corev1.EnvVar{{
					Name:  "OTHER",
					Value: "value",
				}, {
					Name:  consts.OffloadType,
					Value: consts.PVCOffloadType,
				}, {
					Name:  consts.OffloadPVCName,
					Value: "offload-pvc",
				}, {
					Name:  consts.OffloadPath,
					Value: "/offload",
				}},
			}},
			Volumes: []corev1.Volume{{
				Name: "other",
			}, {
				Name: offloadVolumeName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "offload-pvc",
						ReadOnly:  true,
					},
				},
			}},
		},
	}))
}

func TestModifyOutputsFiler(t *testing.T) {
	g := gomega.NewWithT(t)

	podTemplate := &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "task-xxxx",
				VolumeMounts: []corev1.VolumeMount{{
					Name: "other",
				}},
			}},
			Volumes: []corev1.Volume{{
				Name: "other",
			}},
		},
	}

	h := &pvcHelper{path: "/offload", pvcName: "offload-pvc"}
	h.ModifyOutputsFiler("task-xxxx", podTemplate)
	g.Expect(podTemplate).To(gomega.BeEquivalentTo(&corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "task-xxxx",
				VolumeMounts: []corev1.VolumeMount{{
					Name: "other",
				}, {
					Name:      offloadVolumeName,
					MountPath: filepath.Join(h.path, "task-xxxx"),
					SubPath:   "task-xxxx",
					ReadOnly:  true,
				}},
				Env: []corev1.EnvVar{{
					Name:  consts.OffloadType,
					Value: consts.PVCOffloadType,
				}, {
					Name:  consts.OffloadPVCName,
					Value: "offload-pvc",
				}, {
					Name:  consts.OffloadPath,
					Value: "/offload",
				}},
			}},
			Volumes: []corev1.Volume{{
				Name: "other",
			}, {
				Name: offloadVolumeName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "offload-pvc",
						ReadOnly:  true,
					},
				},
			}},
		},
	}))
}
