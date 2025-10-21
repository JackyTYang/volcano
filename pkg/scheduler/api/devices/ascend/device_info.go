/*
Copyright 2023 The Volcano Authors.

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

/*
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

package ascend

import (
	"encoding/json"
	"flag"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/pkg/errors"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"volcano.sh/volcano/pkg/scheduler/api/devices"
	"volcano.sh/volcano/pkg/scheduler/api/devices/config"
	"volcano.sh/volcano/pkg/scheduler/plugins/util/nodelock"
)

const (
	NodeLockAscend         = "hami.io/mutex.lock"
	Ascend910Prefix        = "Ascend910"
	Ascend910NetworkWeight = 10
	// binpack means the lower device memory remained after this allocation, the better
	binpackPolicy = "binpack"
	// spread means better put this task into an idle GPU card than a shared GPU card
	spreadPolicy      = "spread"
	binpackMultiplier = 100
	spreadMultiplier  = 100
)

type AscendDevice struct {
	config           config.VNPUConfig
	nodeRegisterAnno string
	useUUIDAnno      string
	noUseUUIDAnno    string
	handshakeAnno    string
	DeviceInfo       *devices.DeviceInfo
	DeviceUsage      *devices.DeviceUsage
}

type AscendDevices struct {
	NodeName string
	Type     string
	Score    float64
	Devices  map[string]*AscendDevice
	CandicateDevice devices.PodSingleDevice
}

type RuntimeInfo struct {
	UUID string `json:"UUID,omitempty"`
	Temp string `json:"temp,omitempty"`
}

var (
	enableAscend   bool
	configFile     string
	NodeLockEnable bool
)

func NewAscendDevices(name string, node *v1.Node) map[string]*AscendDevices {
	ascend_devices := make(map[string]*AscendDevices)
	if node == nil {
		return ascend_devices
	}
	devs := InitDevices(config.GetConfig().VNPUs)
	for _, dev := range devs {
		node_devices, err := dev.GetNodeDevices(*node)
		if err != nil {
			klog.V(5).InfoS("Failed to get node devices", "nodeName", node.Name, "deviceType", dev.CommonWord(), "error", err)
			continue
		}
		as_devices := &AscendDevices{
			NodeName: name,
			Type:     dev.CommonWord(),
			Devices:  make(map[string]*AscendDevice),
		}
		for _, nd := range node_devices {
			dev.DeviceInfo = nd
			dev.DeviceUsage = &devices.DeviceUsage{
				Used:      0,
				Usedmem:   0,
				Usedcores: 0,
			}
			as_devices.Devices[nd.ID] = dev
		}
		ascend_devices[dev.CommonWord()] = as_devices
	}
	return ascend_devices
}

func (ads *AscendDevices) AddResourceUsage(id string, cores int32, mem int32) error {
	dev, ok := ads.Devices[id]
	if !ok {
		return fmt.Errorf("ascend device %s not found", id)
	}
	dev.DeviceUsage.Used++
	dev.DeviceUsage.Usedcores += cores
	dev.DeviceUsage.Usedmem += mem
	return nil
}

func (ads *AscendDevices) SubResourceUsage(id string, cores int32, mem int32) error {
	dev, ok := ads.Devices[id]
	if !ok {
		return fmt.Errorf("ascend device %s not found", id)
	}
	dev.DeviceUsage.Used--
	dev.DeviceUsage.Usedcores -= cores
	dev.DeviceUsage.Usedmem -= mem
	return nil
}

func (ads *AscendDevices) AddResource(pod *v1.Pod) {
	if ads == nil {
		return
	}
	ads.addResource(pod.Annotations, pod)
}

func (ads *AscendDevices) SubResource(pod *v1.Pod) {
	if ads == nil {
		return
	}
	ano_key := devices.InRequestDevices[ads.Type]
	ano, ok := pod.Annotations[ano_key]
	if !ok {
		klog.Errorf("pod %s has no annotation %s", pod.Name, ano_key)
		return
	}
	con_devs, err := devices.DecodeContainerDevices(ano)
	if err != nil {
		klog.ErrorS(err, "failed to decode container devices", "pod", pod.Name, "annotation", ano)
		return
	}
	for _, cono_dev := range con_devs {
		ads.SubResourceUsage(cono_dev.UUID, cono_dev.Usedcores, cono_dev.Usedmem)
	}
}

func (ads *AscendDevices) addResource(annotations map[string]string, pod *v1.Pod) {
	ano_key := devices.InRequestDevices[ads.Type]
	ano, ok := annotations[ano_key]
	if !ok {
		klog.Errorf("pod %s has no annotation %s", pod.Name, ano_key)
		return
	}
	con_devs, err := devices.DecodeContainerDevices(ano)
	if err != nil {
		klog.ErrorS(err, "failed to decode container devices", "pod", pod.Name, "annotation", ano)
		return
	}
	for _, cono_dev := range con_devs {
		ads.AddResourceUsage(cono_dev.UUID, cono_dev.Usedcores, cono_dev.Usedmem)
	}
}

func (ads *AscendDevices) AddQueueResource(pod *v1.Pod) map[string]float64 {
	return map[string]float64{}
}

func (ads *AscendDevices) HasDeviceRequest(pod *v1.Pod) bool {
	rand_dev, err := ads.getRandomDevice()
	if rand_dev == nil || err != nil {
		return false
	}
	var vnpu_config = rand_dev.config
	for _, container := range pod.Spec.Containers {
		_, ok := container.Resources.Limits[v1.ResourceName(vnpu_config.ResourceName)]
		if ok {
			return true
		}
		_, ok = container.Resources.Limits[v1.ResourceName(vnpu_config.ResourceMemoryName)]
		if ok {
			return true
		}
	}
	return false
}

func getAscendDevicesSnapShot(ads *AscendDevices) *AscendDevices {
	dup_ads := &AscendDevices{
		Devices: make(map[string]*AscendDevice),
	}
	for id, dev := range ads.Devices {
		dup_dev := &AscendDevice{
			config:           dev.config,
			nodeRegisterAnno: dev.nodeRegisterAnno,
			useUUIDAnno:      dev.useUUIDAnno,
			noUseUUIDAnno:    dev.noUseUUIDAnno,
			handshakeAnno:    dev.handshakeAnno,
			DeviceInfo:       dev.DeviceInfo,
			DeviceUsage: &devices.DeviceUsage{
				Used:      dev.DeviceUsage.Used,
				Usedmem:   dev.DeviceUsage.Usedmem,
				Usedcores: dev.DeviceUsage.Usedcores,
			},
		}
		dup_ads.Devices[id] = dup_dev
	}
	return dup_ads
}

func (ads *AscendDevices) FilterNode(pod *v1.Pod, policy string) (int, string, error) {
	devs, err := ads.selectDevices(pod, policy)
	if err != nil {
		return devices.Error, "no ascend device available", err
	}
	klog.V(4).Infoln("ascend DeviceSharing successfully filters pods. device_type:", ads.Type)
	ads.CandicateDevice = devs
	// cal score
	ads.Score = 0
	for _, container_dev := range devs {
		for _, d := range container_dev {
			device_info, ok := ads.Devices[d.UUID]
			if !ok {
				continue
			}
			ads.Score += CalScore(policy, device_info.DeviceUsage, device_info.DeviceInfo)
		}
	}
	return devices.Success, "", nil
}

func (ads *AscendDevices) ScoreNode(pod *v1.Pod, policy string) float64 {
	return ads.Score
}

func (ads *AscendDevices) Allocate(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	klog.V(4).Infoln("hami-vnpu DeviceSharing: Into AllocateToPod", pod.Name)
	if NodeLockEnable {
		nodelock.UseClient(kubeClient)
		err := nodelock.LockNode(ads.NodeName, ads.Type)
		if err != nil {
			return errors.Errorf("node %s locked for %s hamivgpu lockname %s", ads.NodeName, pod.Name, err.Error())
		}
	}
	annotations := make(map[string]string)
	ads.PatchAnnotations(pod, &annotations, ads.CandicateDevice)

	ads.addResource(annotations, pod)
	err := devices.PatchPodAnnotations(kubeClient, pod, annotations)
	if err != nil {
		return err
	}
	if NodeLockEnable {
		nodelock.ReleaseNodeLock(ads.NodeName, ads.Type)
	}
	klog.V(3).Infoln("DeviceSharing:Allocate Success")
	return nil
}

func (ads *AscendDevices) Release(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	return nil
}

func (ads *AscendDevices) GetIgnoredDevices() []string {
	return []string{""}
}

func (ads *AscendDevices) GetStatus() string {
	return ""
}

func (ads *AscendDevices) selectDevices(pod *v1.Pod, schedulePolicy string) (devices.PodSingleDevice, error) {
	rand_dev, err := ads.getRandomDevice()
	if err != nil {
		return nil, errors.Errorf("no ascend device available")
	}
	reqs := rand_dev.ResourceReqs(pod)
	dup_ads := getAscendDevicesSnapShot(ads)
	var pod_devs devices.PodSingleDevice
	for _, req := range reqs {
		var selected_devs devices.ContainerDevices
		for _, dup_ad := range dup_ads.Devices {
			if req.Type != ads.Type {
				continue
			}
			device_usage := dup_ad.DeviceUsage
			device_info := dup_ad.DeviceInfo
			if device_info.Count < device_usage.Used {
				continue
			}
			memreq := int32(0)
			if req.Memreq > 0 {
				memreq = req.Memreq
			} else if req.MemPercentagereq != 101 && req.Memreq == 0 {
				memreq = device_info.Devmem * req.MemPercentagereq / 100
			}
			if device_info.Devmem-device_usage.Usedmem < memreq {
				continue
			}
			if device_info.Devcore-device_usage.Usedcores < req.Coresreq {
				continue
			}
			if device_info.Devcore == 100 && req.Coresreq == 100 && device_usage.Used > 0 {
				continue
			}
			if device_info.Devcore != 0 && device_usage.Usedcores == device_info.Devcore && req.Coresreq == 0 {
				continue
			}
			selected_devs = append(selected_devs, devices.ContainerDevice{
				UUID:      device_info.ID,
				Type:      ads.Type,
				Usedmem:   memreq,
				Usedcores: req.Coresreq,
				CustomInfo: device_info.CustomInfo,
			})
			break
		}
		if len(selected_devs) < int(req.Nums) {
			return nil, errors.Errorf("no enough ascend device available")
		}
		pod_devs = append(pod_devs, selected_devs)
	}
	return pod_devs, nil
}

func (ads *AscendDevices) getRandomDevice() (*AscendDevice, error) {
	if len(ads.Devices) == 0 {
		return nil, errors.New("no ascend device available")
	}
	for _, dev := range ads.Devices {
		return dev, nil
	}
	return nil, errors.New("no ascend device available")
}

func (dev *AscendDevice) trimMemory(m int64) (int64, string) {
	for i := range dev.config.Templates {
		if m <= dev.config.Templates[i].Memory {
			return dev.config.Templates[i].Memory, dev.config.Templates[i].Name
		}
	}
	if m <= dev.config.MemoryCapacity {
		return dev.config.MemoryAllocatable, ""
	}
	return 0, ""
}

func InitDevices(config []config.VNPUConfig) []*AscendDevice {
	var devs []*AscendDevice
	if !enableAscend {
		return devs
	}
	for _, vnpu := range config {
		commonWord := vnpu.CommonWord
		dev := &AscendDevice{
			config:           vnpu,
			nodeRegisterAnno: fmt.Sprintf("hami.io/node-register-%s", commonWord),
			useUUIDAnno:      fmt.Sprintf("hami.io/use-%s-uuid", commonWord),
			noUseUUIDAnno:    fmt.Sprintf("hami.io/no-use-%s-uuid", commonWord),
			handshakeAnno:    fmt.Sprintf("hami.io/node-handshake-%s", commonWord),
		}
		sort.Slice(dev.config.Templates, func(i, j int) bool {
			return dev.config.Templates[i].Memory < dev.config.Templates[j].Memory
		})
		_, ok := devices.InRequestDevices[commonWord]
		if !ok {
			devices.InRequestDevices[commonWord] = fmt.Sprintf("hami.io/%s-devices-to-allocate", commonWord)
			devices.SupportDevices[commonWord] = fmt.Sprintf("hami.io/%s-devices-allocated", commonWord)
			// util.HandshakeAnnos[commonWord] = dev.handshakeAnno
		}
		devs = append(devs, dev)
		klog.Infof("load ascend vnpu config %s: %v", commonWord, dev.config)
	}
	return devs
}

func ParseConfig(fs *flag.FlagSet) {
	fs.BoolVar(&enableAscend, "enable-ascend", false, "enable ascend device")
}

func (dev *AscendDevice) CommonWord() string {
	return dev.config.CommonWord
}

func (dev *AscendDevice) GetNodeDevices(n v1.Node) ([]*devices.DeviceInfo, error) {
	anno, ok := n.Annotations[dev.nodeRegisterAnno]
	if !ok {
		return []*devices.DeviceInfo{}, fmt.Errorf("annos not found %s", dev.nodeRegisterAnno)
	}
	nodeDevices, err := devices.UnMarshalNodeDevices(anno)
	if err != nil {
		klog.ErrorS(err, "failed to unmarshal node devices", "node", n.Name, "device annotation", anno)
		return []*devices.DeviceInfo{}, err
	}
	if len(nodeDevices) == 0 {
		klog.InfoS("no gpu device found", "node", n.Name, "device annotation", anno)
		return []*devices.DeviceInfo{}, errors.New("no device found on node")
	}
	return nodeDevices, nil
}

func (dev *AscendDevice) GenerateResourceRequests(ctr *v1.Container) devices.ContainerDeviceRequest {
	ascendResourceCount := v1.ResourceName(dev.config.ResourceName)
	ascendResourceMem := v1.ResourceName(dev.config.ResourceMemoryName)
	v, ok := ctr.Resources.Limits[ascendResourceCount]
	if !ok {
		v, ok = ctr.Resources.Requests[ascendResourceCount]
	}
	if ok {
		klog.V(3).Infof("Counting %s devices", dev.config.CommonWord)
		if n, ok := v.AsInt64(); ok {
			klog.Info("Found AscendDevices devices")
			memnum := 0
			mem, ok := ctr.Resources.Limits[ascendResourceMem]
			if !ok {
				mem, ok = ctr.Resources.Requests[ascendResourceMem]
			}
			if ok {
				memnums, ok := mem.AsInt64()
				if ok {
					m, _ := dev.trimMemory(memnums)
					memnum = int(m)
				}
			}
			corenum := int32(0)

			mempnum := 0
			if memnum == 0 {
				mempnum = 100
			}

			return devices.ContainerDeviceRequest{
				Nums:             int32(n),
				Type:             dev.CommonWord(),
				Memreq:           int32(memnum),
				MemPercentagereq: int32(mempnum),
				Coresreq:         corenum,
			}
		}
	}
	return devices.ContainerDeviceRequest{}
}

func (dev *AscendDevice) ResourceReqs(pod *v1.Pod) []devices.ContainerDeviceRequest {
	var reqs []devices.ContainerDeviceRequest
	for _, ctr := range pod.Spec.Containers {
		req := dev.GenerateResourceRequests(&ctr)
		if req.Nums > 0 {
			reqs = append(reqs, req)
		}
	}
	return reqs
}

func (ads *AscendDevices) PatchAnnotations(pod *v1.Pod, annoInput *map[string]string, devList devices.PodSingleDevice) map[string]string {
	dev, err := ads.getRandomDevice()
	if err != nil {
		return *annoInput
	}
	commonWord := dev.CommonWord()

	(*annoInput)[devices.InRequestDevices[commonWord]] = devices.EncodePodSingleDevice(devList)
	(*annoInput)[devices.SupportDevices[commonWord]] = devices.EncodePodSingleDevice(devList)
	(*annoInput)["predicate-time"] = strconv.FormatInt(time.Now().Unix(), 10)
	allocateStr := fmt.Sprintf("huawei.com/%s", dev.CommonWord())
	var rtInfo []RuntimeInfo
	for _, dp := range devList {
		for _, val := range dp {
			_, temp := dev.trimMemory(int64(val.Usedmem))
			rtInfo = append(rtInfo, RuntimeInfo{
				UUID: val.UUID,
				Temp: temp,
			})
		}
	}
	s, err := json.Marshal(rtInfo)
	if err != nil {
		klog.ErrorS(err, "failed to marshal runtime info", "runtime info", rtInfo)
	}
	(*annoInput)[allocateStr] = string(s)

	return *annoInput
}

func (dev *AscendDevice) GetResourceNames() devices.ResourceNames {
	return devices.ResourceNames{
		ResourceCountName:  dev.config.ResourceName,
		ResourceMemoryName: dev.config.ResourceMemoryName,
		ResourceCoreName:   "",
	}
}

func CalScore(schedulePolicy string, dev_usage *devices.DeviceUsage, dev_info *devices.DeviceInfo) float64 {
	var score float64
	switch schedulePolicy {
	case binpackPolicy:
		score = binpackMultiplier * (float64(dev_usage.Usedmem) / float64(dev_info.Devmem))
	case spreadPolicy:
		if dev_usage.Used == 1 {
			score = spreadMultiplier
		}
	default:
		score = float64(0)
	}
	return score
}
