/*
Copyright 2024 The Volcano Authors.

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

package deviceshare

import (
	"context"
	"fmt"
	"math"
	"reflect"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/devices"
	"volcano.sh/volcano/pkg/scheduler/api/devices/ascend/ascend310p/vnpu"
	"volcano.sh/volcano/pkg/scheduler/api/devices/nvidia/gpushare"
	"volcano.sh/volcano/pkg/scheduler/api/devices/nvidia/vgpu"
	"volcano.sh/volcano/pkg/scheduler/framework"
	vnpu310p "volcano.sh/volcano/pkg/scheduler/plugins/deviceshare/policy/ascend/310p/vnpu"
)

// PluginName indicates name of volcano scheduler plugin.
const (
	PluginName = "deviceshare"
	// GPUSharingPredicate is the key for enabling GPU Sharing Predicate in YAML
	GPUSharingPredicate = "deviceshare.GPUSharingEnable"
	NodeLockEnable      = "deviceshare.NodeLockEnable"
	GPUNumberPredicate  = "deviceshare.GPUNumberEnable"

	VGPUEnable = "deviceshare.VGPUEnable"

	ASCEND310PvGPU = "deviceshare.ASCEND310PVGPUEnable"

	SchedulePolicyArgument = "deviceshare.SchedulePolicy"
	ScheduleWeight         = "deviceshare.ScheduleWeight"
)

type deviceSharePlugin struct {
	// Arguments given for the plugin
	pluginArguments framework.Arguments
	schedulePolicy  string
	scheduleWeight  int
}

// New return priority plugin
func New(arguments framework.Arguments) framework.Plugin {
	dsp := &deviceSharePlugin{pluginArguments: arguments, schedulePolicy: "", scheduleWeight: 0}
	enablePredicate(dsp)
	return dsp
}

func (dp *deviceSharePlugin) Name() string {
	return PluginName
}

func enablePredicate(dsp *deviceSharePlugin) {
	// Checks whether predicate.GPUSharingEnable is provided or not, if given, modifies the value in predicateEnable struct.
	args := dsp.pluginArguments
	args.GetBool(&gpushare.GpuSharingEnable, GPUSharingPredicate)
	args.GetBool(&gpushare.GpuNumberEnable, GPUNumberPredicate)
	args.GetBool(&gpushare.NodeLockEnable, NodeLockEnable)
	args.GetBool(&vgpu.VGPUEnable, VGPUEnable)
	args.GetBool(&vnpu.Ascend310pvNPUEnable, ASCEND310PvGPU)

	_, ok := args[SchedulePolicyArgument]
	if ok {
		dsp.schedulePolicy = args[SchedulePolicyArgument].(string)
	}
	args.GetInt(&dsp.scheduleWeight, ScheduleWeight)

	if gpushare.GpuSharingEnable && gpushare.GpuNumberEnable {
		klog.Fatal("can not define true in both gpu sharing and gpu number")
	}
	if (gpushare.GpuSharingEnable || gpushare.GpuNumberEnable) && vgpu.VGPUEnable {
		klog.Fatal("gpu-share and vgpu can't be used together")
	}
}

func createStatus(code int, reason string) *api.Status {
	status := api.Status{
		Code:   code,
		Reason: reason,
	}
	return &status
}

func getDeviceScore(ctx context.Context, pod *v1.Pod, node *api.NodeInfo, schedulePolicy string) (int64, *k8sframework.Status) {
	s := float64(0)
	for _, devices := range node.Others {
		if devices.(api.Devices).HasDeviceRequest(pod) {
			ns := devices.(api.Devices).ScoreNode(pod, schedulePolicy)
			s += ns
		}
	}
	klog.V(4).Infof("deviceScore for task %s/%s is: %v", pod.Namespace, pod.Name, s)
	return int64(math.Floor(s + 0.5)), nil
}

func ScoreBatchNodes(pod *v1.Pod, schedulePolicy string, device api.Devices, neighbours []api.Devices) []float64 {
	switch d := device.(type) {
	case *vnpu.NPUDevices:
		// if you need to rewrite your score policy, add a case here
		return vnpu310p.ScoreBatchNodes(pod, schedulePolicy, d, neighbours)
	default:
		score := make([]float64, 0)
		score = append(score, device.ScoreNode(pod, schedulePolicy))
		return score
	}
}

func initScoreMap(nodes []*api.NodeInfo) map[string]float64 {
	scoreMap := make(map[string]float64, len(nodes))
	for _, node := range nodes {
		if reflect.ValueOf(node).IsNil() {
			continue
		}
		scoreMap[node.Name] = 0.0
	}
	return scoreMap
}

func initializeDevicesWithSession(ssn *framework.Session) {
	for _, nodeInfo := range ssn.Nodes { // initialize every device in every node with global ssn
		for _, val := range api.RegisteredDevices {
			if dev, ok := nodeInfo.Others[val].(api.Devices); ok {
				if err := initializeDevice(dev, ssn, nodeInfo); err != nil {
					klog.Warningf("Failed to initialize devices with session for node %s: %v", nodeInfo.Name, err)
				}
			}
		}
	}
}

// initialization function for different devices
func initializeDevice(device api.Devices, ssn *framework.Session, nodeInfo *api.NodeInfo) error {
	switch d := device.(type) {
	case *vnpu.NPUDevices:
		return vnpu310p.InitVNPUDevice(d, ssn, nodeInfo)
	default:
		return nil
	}
}

func (dp *deviceSharePlugin) OnSessionOpen(ssn *framework.Session) {
	// initialize devices which needs ssn as input
	initializeDevicesWithSession(ssn)

	// Register event handlers to update task info in PodLister & nodeMap
	ssn.AddPredicateFn(dp.Name(), func(task *api.TaskInfo, node *api.NodeInfo) ([]*api.Status, error) {
		predicateStatus := make([]*api.Status, 0)
		// Check PredicateWithCache
		for _, val := range api.RegisteredDevices {
			if dev, ok := node.Others[val].(api.Devices); ok {
				if reflect.ValueOf(dev).IsNil() {
					// TODO When a pod requests a device of the current type, but the current node does not have such a device, an error is thrown
					if dev == nil || dev.HasDeviceRequest(task.Pod) {
						predicateStatus = append(predicateStatus, &api.Status{
							Code:   devices.Unschedulable,
							Reason: "node not initialized with device" + val,
						})
						return predicateStatus, fmt.Errorf("node not initialized with device %s", val)
					}
					klog.V(4).Infof("pod %s/%s did not request device %s on %s, skipping it", task.Pod.Namespace, task.Pod.Name, val, node.Name)
					continue
				}
				code, msg, err := dev.FilterNode(task.Pod, dp.schedulePolicy)
				if err != nil {
					predicateStatus = append(predicateStatus, createStatus(code, msg))
					return predicateStatus, err
				}
				filterNodeStatus := createStatus(code, msg)
				if filterNodeStatus.Code != api.Success {
					predicateStatus = append(predicateStatus, filterNodeStatus)
					return predicateStatus, fmt.Errorf("plugin device filternode predicates failed %s", msg)
				}
			} else {
				klog.Warningf("Devices %s assertion conversion failed, skip", val)
			}
		}

		klog.V(4).Infof("checkDevices predicates Task <%s/%s> on Node <%s>: fit ",
			task.Namespace, task.Name, node.Name)

		return predicateStatus, nil
	})

	ssn.AddNodeOrderFn(dp.Name(), func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		// DeviceScore
		if len(dp.schedulePolicy) > 0 {
			score, status := getDeviceScore(context.TODO(), task.Pod, node, dp.schedulePolicy)
			if !status.IsSuccess() {
				klog.Warningf("Node: %s, Calculate Device Score Failed because of Error: %v", node.Name, status.AsError())
				return 0, status.AsError()
			}

			// TODO: we should use a seperate plugin for devices, and seperate them from predicates and nodeOrder plugin.
			nodeScore := float64(score) * float64(dp.scheduleWeight)
			klog.V(5).Infof("Node: %s, task<%s/%s> Device Score weight %d, score: %f", node.Name, task.Namespace, task.Name, dp.scheduleWeight, nodeScore)
		}
		return 0, nil
	})

	ssn.AddBatchNodeOrderFn(dp.Name(), func(task *api.TaskInfo, nodes []*api.NodeInfo) (map[string]float64, error) {
		scoreMap := initScoreMap(nodes)
		for _, deviceType := range api.RegisteredDevices {
			//get all nodes' this device
			//neighbours store all devices of the global nodes
			neighbours := make([]api.Devices, 0)
			for _, node := range nodes {
				device, ok := node.Others[deviceType]
				if ok {
					if deviceInterface, isDeviceInterface := device.(api.Devices); isDeviceInterface {
						neighbours = append(neighbours, deviceInterface)
					}
				}
			}
			for _, node := range nodes {
				if dev, ok := node.Others[deviceType].(api.Devices); ok {
					if reflect.ValueOf(dev).IsNil() {
						// TODO When a pod requests a device of the current type, but the current node does not have such a device, an error is thrown
						if dev == nil || dev.HasDeviceRequest(task.Pod) {

							return nil, fmt.Errorf("node not initialized with device %s", deviceType)
						}
						klog.V(4).Infof("pod %s/%s did not request device %s on %s, skipping it", task.Pod.Namespace, task.Pod.Name, deviceType, nodes[0].Name)
						continue
					}
					score := ScoreBatchNodes(task.Pod, dp.schedulePolicy, dev, neighbours)
					if len(score) > 1 {
						for i := range nodes {
							scoreMap[nodes[i].Node.Name] += score[i]
						}
						break
					}
					scoreMap[node.Name] += score[0]
				} else {
					klog.Warningf("Devices %s assertion conversion failed, skip", deviceType)
				}
			}
		}

		return scoreMap, nil
	})
}

func (dp *deviceSharePlugin) OnSessionClose(ssn *framework.Session) {}
