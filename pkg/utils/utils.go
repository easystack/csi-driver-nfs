package utils

import (
	b64 "encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

const (
	mountInfoFileName = "mountInfo.json"
	nfsDir            = "nfs"
)

// FSGroupChangePolicy holds policies that will be used for applying fsGroup to a volume.
// This type and the allowed values are tracking the PodFSGroupChangePolicy defined in
// https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/api/core/v1/types.go
// It is up to the client using the direct-assigned volume feature (e.g. CSI drivers) to determine
// the optimal setting for this change policy (i.e. from Pod spec or assuming volume ownership
// based on the storage offering).
type FSGroupChangePolicy string

var kataDirectVolumeRootPath = "/run/kata-containers/shared/direct-volumes"

// MountInfo contains the information needed by Kata to consume a host block device and mount it as a filesystem inside the guest VM.
type MountInfo struct {
	// The type of the volume (ie. block)
	VolumeType string `json:"volume-type"`
	// The device backing the volume.
	Device string `json:"device"`
	// The filesystem type to be mounted on the volume.
	FsType string `json:"fstype"`
	// Additional metadata to pass to the agent regarding this volume.
	Metadata map[string]string `json:"metadata,omitempty"`
	// Additional mount options.
	Options []string `json:"options,omitempty"`
}

// Add writes the mount info of a direct volume into a filesystem path known to Kata Container.
func Add(volumePath, mountInfo string) error {
	volumeDir := filepath.Join(kataDirectVolumeRootPath, nfsDir, b64.URLEncoding.EncodeToString([]byte(volumePath)))
	stat, err := os.Stat(volumeDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.MkdirAll(volumeDir, 0700); err != nil {
			return err
		}
	}
	if stat != nil && !stat.IsDir() {
		return fmt.Errorf("%s should be a directory", volumeDir)
	}

	var deserialized MountInfo
	if err := json.Unmarshal([]byte(mountInfo), &deserialized); err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(volumeDir, mountInfoFileName), []byte(mountInfo), 0600)
}

// Remove deletes the direct volume path including all the files inside it.
func Remove(volumePath string) error {
	return os.RemoveAll(filepath.Join(kataDirectVolumeRootPath, nfsDir, b64.URLEncoding.EncodeToString([]byte(volumePath))))
}

func AddDirectVolume(volumePath, server, baseDir, fsType string) error {
	mountInfo := struct {
		VolumeType string            `json:"volume-type"`
		Device     string            `json:"device"`
		FsType     string            `json:"fstype"`
		Metadata   map[string]string `json:"metadata,omitempty"`
		Options    []string          `json:"options,omitempty"`
	}{
		VolumeType: "fs",
		FsType:     fsType,
		Options:    []string{server + ":" + baseDir},
	}

	mi, err := json.Marshal(mountInfo)
	if err != nil {
		klog.Errorf("addDirectVolume - json.Marshal failed: ", err.Error())
		return status.Errorf(codes.Internal, "json.Marshal failed: %s", err.Error())
	}

	if err := Add(volumePath, string(mi)); err != nil {
		klog.Errorf("addDirectVolume - add direct volume failed: ", err.Error())
		return status.Errorf(codes.Internal, "add direct volume failed: %s", err.Error())
	}

	klog.Infof("add direct volume done: %s %s", volumePath, string(mi))
	return nil
}
