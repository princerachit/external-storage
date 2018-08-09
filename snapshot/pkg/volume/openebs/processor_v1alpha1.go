/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package openebs

import (
	"fmt"
	"os"

	mvol_v1alpha1 "github.com/kubernetes-incubator/external-storage/openebs/pkg/volume/v1alpha1"

	"github.com/golang/glog"
	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/kubernetes-incubator/external-storage/snapshot/pkg/cloudprovider"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type openEBSv1alpha1Plugin struct {
	openEBSPluginInterface
	mvol_v1alpha1.CASVolume
}

func (h *openEBSv1alpha1Plugin) Init(_ cloudprovider.Interface) {
}

func (h *openEBSv1alpha1Plugin) SnapshotCreate(snapshot *crdv1.VolumeSnapshot, pv *v1.PersistentVolume, tags *map[string]string) (*crdv1.VolumeSnapshotDataSource, *[]crdv1.VolumeSnapshotCondition, error) {
	spec := &pv.Spec
	if spec == nil || spec.ISCSI == nil {
		return nil, nil, fmt.Errorf("invalid PV spec %v", spec)
	}

	snapObj := (*tags)["kubernetes.io/created-for/snapshot/name"]
	snapshotName := createSnapshotName(pv.Name, snapObj)
	_, err := h.CreateSnapshot(pv.Name, snapshotName, pv.Spec.ClaimRef.Namespace)
	if err != nil {
		glog.Errorf("failed to create snapshot for volume :%v, err: %v", pv.Name, err)
		return nil, nil, err
	}
	glog.V(1).Info("snapshot %v created successfully", snapshotName)

	cond := []crdv1.VolumeSnapshotCondition{}
	if err == nil {
		cond = []crdv1.VolumeSnapshotCondition{
			{
				Status:             v1.ConditionTrue,
				Message:            "Snapshot created successfully",
				LastTransitionTime: metav1.Now(),
				Type:               crdv1.VolumeSnapshotConditionReady,
			},
		}
	} else {
		glog.V(2).Infof("failed to create snapshot, err: %v", err)
		cond = []crdv1.VolumeSnapshotCondition{
			{
				Status:             v1.ConditionTrue,
				Message:            fmt.Sprintf("Failed to create the snapshot: %v", err),
				LastTransitionTime: metav1.Now(),
				Type:               crdv1.VolumeSnapshotConditionError,
			},
		}
	}

	res := &crdv1.VolumeSnapshotDataSource{
		OpenEBSSnapshot: &crdv1.OpenEBSVolumeSnapshotSource{
			SnapshotID: snapshotName,
		},
	}
	return res, &cond, err
}

func (h *openEBSv1alpha1Plugin) SnapshotDelete(src *crdv1.VolumeSnapshotDataSource, pv *v1.PersistentVolume) error {
	if src == nil || src.OpenEBSSnapshot == nil {
		return fmt.Errorf("invalid VolumeSnapshotDataSource: %v", src)
	}
	snapshotID := src.OpenEBSSnapshot.SnapshotID
	glog.V(1).Infof("Received snapshot :%v delete request", snapshotID)

	_, err := h.DeleteSnapshot(snapshotID)
	if err != nil {
		glog.Errorf("failed to delete snapshot: %v, err: %v", snapshotID, err)
	}

	glog.V(1).Infof("snapshot deleted :%v successfully", snapshotID)
	return err
}

func (h *openEBSv1alpha1Plugin) DescribeSnapshot(snapshotData *crdv1.VolumeSnapshotData) (snapConditions *[]crdv1.VolumeSnapshotCondition, isCompleted bool, err error) {
	if snapshotData == nil || snapshotData.Spec.OpenEBSSnapshot == nil {
		return nil, false, fmt.Errorf("failed to retrieve Snapshot spec")
	}

	snapshotID := snapshotData.Spec.OpenEBSSnapshot.SnapshotID
	glog.V(1).Infof("received describe request on snapshot:%v", snapshotID)

	// TODO implement snapshot-info based on snapshotID
	resp, err := h.SnapshotInfo(snapshotData.Spec.PersistentVolumeRef.Name, snapshotID)

	if err != nil {
		glog.Errorf("failed to describe snapshot:%v", snapshotID)
	}

	glog.V(1).Infof("snapshot details:%v", string(resp))

	if len(snapshotData.Status.Conditions) == 0 {
		return nil, false, fmt.Errorf("No status condtions in VoluemSnapshotData for openebs snapshot type")
	}

	lastCondIdx := len(snapshotData.Status.Conditions) - 1
	retCondType := crdv1.VolumeSnapshotConditionError

	switch snapshotData.Status.Conditions[lastCondIdx].Type {
	case crdv1.VolumeSnapshotDataConditionReady:
		retCondType = crdv1.VolumeSnapshotConditionReady
	case crdv1.VolumeSnapshotDataConditionPending:
		retCondType = crdv1.VolumeSnapshotConditionPending
		// Error out.
	}
	retCond := []crdv1.VolumeSnapshotCondition{
		{
			Status:             snapshotData.Status.Conditions[lastCondIdx].Status,
			Message:            snapshotData.Status.Conditions[lastCondIdx].Message,
			LastTransitionTime: snapshotData.Status.Conditions[lastCondIdx].LastTransitionTime,
			Type:               retCondType,
		},
	}
	return &retCond, true, nil
}

// FindSnapshot finds a VolumeSnapshot by matching metadata
func (h *openEBSv1alpha1Plugin) FindSnapshot(tags *map[string]string) (*crdv1.VolumeSnapshotDataSource, *[]crdv1.VolumeSnapshotCondition, error) {
	glog.Infof("FindSnapshot by tags: %#v", *tags)

	// TODO: Implement FindSnapshot
	return nil, nil, fmt.Errorf("Snapshot not found")
}

// SnapshotRestore restore to any created snapshot
func (h *openEBSv1alpha1Plugin) SnapshotRestore(snapshotData *crdv1.VolumeSnapshotData,
	pvc *v1.PersistentVolumeClaim,
	pvName string,
	parameters map[string]string,
) (*v1.PersistentVolumeSource, map[string]string, error) {
	// TODO: implement
	return nil, nil, nil
}

// VolumeDelete deletes the persistent volume
func (h *openEBSv1alpha1Plugin) VolumeDelete(pv *v1.PersistentVolume) error {
	if pv == nil || pv.Spec.ISCSI == nil {
		return fmt.Errorf("invalid VolumeSnapshotDataSource: %v", pv)
	}
	var openebsVol mvol_v1alpha1.CASVolume

	err := openebsVol.DeleteVolume(pv.Name, pv.Spec.ClaimRef.Namespace)
	if err != nil {
		glog.Errorf("Error while deleting volume: %v", err)
		return err
	}
	return nil
}

func (h *openEBSv1alpha1Plugin) GetMayaService() error {
	client, err := GetK8sClient()
	if err != nil {
		return err
	}
	//Get maya-apiserver IP address from cluster
	addr, err := h.GetMayaClusterIP(client)
	if err != nil {
		glog.Errorf("Error getting maya-apiserver IP Address: %v", err)
		return err
	}
	mayaServiceURI := "http://" + addr + ":5656"
	//Set maya-apiserver IP address along with default port
	os.Setenv("MAPI_ADDR", mayaServiceURI)

	return nil
}
