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

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"syscall"

	"github.com/golang/glog"
	"github.com/kubernetes-incubator/external-storage/lib/controller"
	"github.com/kubernetes-incubator/external-storage/lib/util"
	mApiv1 "github.com/kubernetes-incubator/external-storage/openebs/pkg/v1"
	mayav1 "github.com/kubernetes-incubator/external-storage/openebs/types/v1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	env_util "github.com/kubernetes-incubator/external-storage/openebs/pkg/util"
)

const (
	provisionerName = "openebs.io/provisioner-iscsi"
	// BetaStorageClassAnnotation represents the beta/previous StorageClass annotation.
	// It's currently still used and will be held for backwards compatibility
	BetaStorageClassAnnotation = "volume.beta.kubernetes.io/storage-class"
	//defaultFSType
	defaultFSType = "ext4"
)

// validFSType represents the valid fstype supported by openebs volume
// New supported type can be added using OPENEBS_VALID_FSTYPE env
var validFSType = []string{"ext4", "xfs"}

type openEBSProvisioner struct {
	// Maya-API Server URI running in the cluster
	mapiURI string

	// Identity of this openEBSProvisioner, set to node's name. Used to identify
	// "this" provisioner's PVs.
	identity string
}

// NewOpenEBSProvisioner creates a new openebs provisioner
func NewOpenEBSProvisioner(client kubernetes.Interface) controller.Provisioner {

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		glog.Errorf("ENV variable 'NODE_NAME' is not set")
	}
	var openebsObj mApiv1.OpenEBSVolume

	//Get maya-apiserver IP address from cluster
	addr, err := openebsObj.GetMayaClusterIP(client)

	if err != nil {
		glog.Errorf("Error getting maya-apiserver IP Address: %v", err)
		return nil
	}
	mayaServiceURI := "http://" + addr + ":5656"

	//Set maya-apiserver IP address along with default port
	os.Setenv("MAPI_ADDR", mayaServiceURI)

	return &openEBSProvisioner{
		mapiURI:  mayaServiceURI,
		identity: nodeName,
	}
}

var _ controller.Provisioner = &openEBSProvisioner{}

// Provision creates a storage asset and returns a PV object representing it.
func (p *openEBSProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {

	//Issue a request to Maya API Server to create a volume
	var volume mayav1.Volume
	var openebsVol mApiv1.OpenEBSVolume
	volumeSpec := mayav1.VolumeSpec{}

	volSize := options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	volumeSpec.Metadata.Labels.Storage = volSize.String()

	className := GetStorageClassName(options)

	if className == nil {
		glog.Errorf("Volume has no storage class specified")
	} else {
		volumeSpec.Metadata.Labels.StorageClass = *className
	}
	volumeSpec.Metadata.Labels.Namespace = options.PVC.Namespace
	volumeSpec.Metadata.Labels.PersistentVolumeClaim = options.PVC.ObjectMeta.Name
	volumeSpec.Metadata.Name = options.PVName

	//Pass through labels from PVC to maya-apiserver
	volumeSpec.Metadata.Labels.Application = options.PVC.ObjectMeta.GetLabels()[mayav1.PVCLabelsApplication]
	volumeSpec.Metadata.Labels.ReplicaTopoKeyDomain = options.PVC.ObjectMeta.GetLabels()[mayav1.PVCLabelsReplicaTopKeyDomain]
	volumeSpec.Metadata.Labels.ReplicaTopoKeyType = options.PVC.ObjectMeta.GetLabels()[mayav1.PVCLabelsReplicaTopKeyType]

	_, err := openebsVol.CreateVolume(volumeSpec)
	if err != nil {
		glog.Errorf("Error creating volume: %v", err)
		return nil, err
	}

	err = openebsVol.ListVolume(options.PVName, options.PVC.Namespace, &volume)
	if err != nil {
		glog.Errorf("Error getting volume details: %v", err)
		return nil, err
	}

	// Use annotations to specify the context using which the PV was created.
	volAnnotations := make(map[string]string)
	volAnnotations["openEBSProvisionerIdentity"] = p.identity

	var iqn, targetPortal string

	for key, value := range volume.Metadata.Annotations.(map[string]interface{}) {
		switch key {
		case "vsm.openebs.io/iqn":
			iqn = value.(string)
		case "vsm.openebs.io/targetportals":
			targetPortal = value.(string)
		}
	}

	glog.V(2).Infof("Volume IQN: %v , Volume Target: %v", iqn, targetPortal)

	if !util.AccessModesContainedInAll(p.GetAccessModes(), options.PVC.Spec.AccessModes) {
		glog.V(1).Info("Invalid Access Modes: %v, Supported Access Modes: %v", options.PVC.Spec.AccessModes, p.GetAccessModes())
		return nil, fmt.Errorf("Invalid Access Modes: %v, Supported Access Modes: %v", options.PVC.Spec.AccessModes, p.GetAccessModes())
	}

	// The following will be used by the dashboard, to display links on PV page
	userLinks := make([]string, 0)
	localMonitoringURL := os.Getenv("OPENEBS_MONITOR_URL")
	if localMonitoringURL != "" {
		localMonitorLinkName := os.Getenv("OPENEBS_MONITOR_LINK_NAME")
		if localMonitorLinkName == "" {
			localMonitorLinkName = "monitor"
		}
		localMonitorVolKey := os.Getenv("OPENEBS_MONITOR_VOLKEY")
		if localMonitorVolKey != "" {
			localMonitoringURL += localMonitorVolKey + "=" + options.PVName
		}
		userLinks = append(userLinks, "\""+localMonitorLinkName+"\":\""+localMonitoringURL+"\"")
	}
	mayaPortalURL := os.Getenv("MAYA_PORTAL_URL")
	if mayaPortalURL != "" {
		mayaPortalLinkName := os.Getenv("MAYA_PORTAL_LINK_NAME")
		if mayaPortalLinkName == "" {
			mayaPortalLinkName = "maya"
		}
		userLinks = append(userLinks, "\""+mayaPortalLinkName+"\":\""+mayaPortalURL+"\"")
	}
	if len(userLinks) > 0 {
		volAnnotations["alpha.dashboard.kubernetes.io/links"] = "{" + strings.Join(userLinks, ",") + "}"
	}

	fsType, err := parseClassParameters(options.Parameters)
	if err != nil {
		return nil, err
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:        options.PVName,
			Annotations: volAnnotations,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: options.PersistentVolumeReclaimPolicy,
			AccessModes:                   options.PVC.Spec.AccessModes,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				ISCSI: &v1.ISCSIPersistentVolumeSource{
					TargetPortal: targetPortal,
					IQN:          iqn,
					Lun:          0,
					FSType:       fsType,
					ReadOnly:     false,
				},
			},
		},
	}

	return pv, nil
}

// Delete removes the storage asset that was created by Provision represented
// by the given PV.
func (p *openEBSProvisioner) Delete(volume *v1.PersistentVolume) error {

	var openebsVol mApiv1.OpenEBSVolume

	ann, ok := volume.Annotations["openEBSProvisionerIdentity"]
	if !ok {
		return errors.New("identity annotation not found on PV")
	}
	if ann != p.identity {
		return &controller.IgnoredError{Reason: "identity annotation on PV does not match ours"}
	}

	// Issue a delete request to Maya API Server
	err := openebsVol.DeleteVolume(volume.Name, volume.Spec.ClaimRef.Namespace)
	if err != nil {
		glog.Errorf("Error while deleting volume: %v", err)
		return err
	}

	return nil
}

func (p *openEBSProvisioner) GetAccessModes() []v1.PersistentVolumeAccessMode {
	return []v1.PersistentVolumeAccessMode{
		v1.ReadWriteOnce,
	}
}

func main() {
	var provisioner controller.Provisioner
	syscall.Umask(0)

	flag.Parse()
	flag.Set("logtostderr", "true")
	var (
		config     *rest.Config
		err        error
		k8sMaster  = mayav1.K8sMasterENV()
		kubeConfig = mayav1.KubeConfigENV()
	)
	if len(k8sMaster) != 0 || len(kubeConfig) != 0 {
		fmt.Printf("Build client config using k8s Master's Address: '%s' or Kubeconfig: '%s' \n", k8sMaster, kubeConfig)
		config, err = clientcmd.BuildConfigFromFlags(k8sMaster, kubeConfig)
	} else {
		// Create an InClusterConfig and use it to create a client for the controller
		// to use to communicate with Kubernetes
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		glog.Errorf("Failed to create config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Errorf("Failed to create client: %v", err)
	}

	// The controller needs to know what the server version is because out-of-tree
	// provisioners aren't officially supported until 1.5
	serverVersion, err := clientset.Discovery().ServerVersion()
	if err != nil {
		glog.Errorf("Error getting server version: %v", err)
	}

	// Create the provisioner: it implements the Provisioner interface expected by
	// the controller.
	// Create v1alpha1 provisioner if gated feature is on
	if env_util.CASTemplateFeatureGate() {
		provisioner = NewOpenEBSProvisionerV1alpha1(clientset)
	} else {
		provisioner = NewOpenEBSProvisioner(clientset)
	}

	if provisioner != nil {
		// Start the provision controller which will dynamically provision OpenEBS VSM
		// PVs
		pc := controller.NewProvisionController(
			clientset,
			provisionerName,
			provisioner,
			serverVersion.GitVersion)

		pc.Run(wait.NeverStop)
	} else {
		os.Exit(1) //Exit if provisioner not created.
	}

}

// GetPersistentVolumeClass returns StorageClassName.
func GetStorageClassName(options controller.VolumeOptions) *string {
	// Use beta annotation first
	if class, found := options.PVC.Annotations[BetaStorageClassAnnotation]; found {
		return &class
	}
	return options.PVC.Spec.StorageClassName
}

// parseClassParameters extract the new fstype other then "ext4"(dafault) which
// can be changed via "openebs.io/fstype" key and env OPENEBS_VALID_FSTYPE
func parseClassParameters(params map[string]string) (string, error) {
	var fsType string
	for k, v := range params {
		switch strings.ToLower(k) {
		case "openebs.io/fstype":
			fsType = v
		}
	}
	if len(fsType) == 0 {
		fsType = defaultFSType
	}

	//Get openebs supported fstype from ENV variable
	validENVFSType := os.Getenv("OPENEBS_VALID_FSTYPE")
	if validENVFSType != "" {
		slices := strings.Split(validENVFSType, ",")
		for _, s := range slices {
			validFSType = append(validFSType, s)
		}
	}
	if !isValid(fsType, validFSType) {
		return "", fmt.Errorf("Filesystem %s is not supported", fsType)
	}
	return fsType, nil
}

// isValid checks the validity of fstype returns true if supported
func isValid(value string, list []string) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}
