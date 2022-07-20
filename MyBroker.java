package org.cloudbus.cloudsim.examples.UtilizationScheduling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.examples.UtilizationScheduling.WorkFlowParser.Node;


public class MyBroker extends DatacenterBroker {

	private static final int PROVISION = 10001;

	private int provisionInterval = 120;
	
	private int completedTaskCount = 0;

	private int vmId = 0;

	private int datacenterId = 2;

	private ArrayList<Integer> idleVmIdList;
	private Map<Integer, Vm> id2VmMap;

	private Map<Integer, Double> id2VmStartTime;
	private Map<Integer, Double> id2VmTermiateTime;

	private HashMap<String, WorkFlowParser.Node> nodes;
	private WorkFlowParser workflow;
	private int num_tasks;
	private int workflow_depth;
	private int numberOfVms;

	private ArrayList<Cloudlet> taskList;
	private ArrayList<String> runningTaskIds;

	private HashMap<Integer, String> cloudletId2nodeId;

	private int cloudletId = 0;

	private boolean firstInit = true;

	private HashMap<Integer, ArrayList<Double>> vmScheduledTimes = new HashMap<Integer, ArrayList<Double>>();
	private HashMap<Integer, ArrayList<Double>> vmMadeFreeTimes = new HashMap<Integer, ArrayList<Double>>();
	
	private boolean endLoop = false;

	private ArrayList<Integer> allVmsId = new ArrayList<Integer>();

	public MyBroker(String name) throws Exception {
		super(name);
	}

	@Override
	public void startEntity() {
		workflow = new WorkFlowParser("SIPHT_500_1.xml");
		idleVmIdList = new ArrayList<>();
		id2VmMap = new HashMap<>();
		taskList = new ArrayList<>();
		nodes = new HashMap<String, WorkFlowParser.Node>();
		cloudletId2nodeId = new HashMap<Integer,String>();
		nodes = workflow.getNodes();
		num_tasks = workflow.getNodes().size();
		workflow_depth = workflow.getDepth();
		Log.printLine("number of tasks = "+num_tasks);
		Log.printLine("workflow depth = "+workflow_depth);
		numberOfVms = (int) Math.ceil((float) num_tasks / workflow_depth);
		Log.printLine("VMs count = " + numberOfVms);
		runningTaskIds = new ArrayList<>();
		id2VmStartTime = new HashMap<>();
		id2VmTermiateTime = new HashMap<>();
		
		send(getId(), provisionInterval, PROVISION);
		
	}
	

	@Override
	public void processEvent(SimEvent ev) {
		switch (ev.getTag()) {

		// VM Creation answer
		case CloudSimTags.VM_CREATE_ACK:
			processVmCreate(ev);
			scheduleTasks();
			break;
		// VM Creation answer
		case CloudSimTags.VM_DESTROY_ACK:
			processVmTerminate(ev);
			break;
		// A finished cloudlet returned
		case CloudSimTags.CLOUDLET_RETURN:
			processCloudletReturn(ev);
			listPrepareTasks();
			scheduleTasks();
			break;
		// if the simulation finishes
		case CloudSimTags.END_OF_SIMULATION:
			shutdownEntity();
			break;
			
		case PROVISION:
			if(!endLoop){
				handleGreedyScheduling();
			}
			break;
		// other unknown tags are processed by this method
		default:
			processOtherEvent(ev);
			break;
		}
	}

	protected void handleGreedyScheduling() {

		if (completedTaskCount==num_tasks) {
			return;
		}

		send(getId(), provisionInterval, PROVISION);


		if(firstInit) {
			listPrepareTasks();
			for (int i = 0; i < numberOfVms; i++) {
				createVm();
			}
			firstInit = false;

		} else {

			double vmsRunningTime = 0.0;
			double allTime = 0.0;


			for(Integer vmId : id2VmMap.keySet()) {

				//Calcute all vms running time.
				if(vmScheduledTimes.get(vmId).size() == 0){
					
				} else {
					ArrayList<Double> vmSdList = vmScheduledTimes.get(vmId);
					ArrayList<Double> vmMfList = vmMadeFreeTimes.get(vmId);
	
					double currentTime = CloudSim.clock();
					double lastTime = CloudSim.clock() - 120.0;
					
	
					if (vmSdList.size() == vmMfList.size()) {
	
						for(int lastIndex = vmSdList.size()-1; lastIndex >= 0;lastIndex--) {
	
							if (vmSdList.get(lastIndex) <= lastTime && vmMfList.get(lastIndex) <= lastTime) {
								double runningTime = 0;
								vmsRunningTime = vmsRunningTime + runningTime;
								
							} else {
								double runningTime = getIntersection(currentTime, lastTime,vmSdList.get(lastIndex), vmMfList.get(lastIndex));
								vmsRunningTime = vmsRunningTime + runningTime;
							}
	
						}
	
					} else if(vmSdList.size() == vmMfList.size() + 1 ) {
	
						for(int lastIndex = vmSdList.size()-1; lastIndex >= 0;lastIndex--) {
							double runningTime = 0.0;
	
							if(lastIndex == vmSdList.size()-1){
								runningTime = getIntersection(currentTime, lastTime,vmSdList.get(lastIndex), -5);
							} else {
								if (vmSdList.get(lastIndex) <= lastTime && vmMfList.get(lastIndex) <= lastTime) {
									runningTime = 0.0;
								} else {
									runningTime = getIntersection(currentTime, lastTime,vmSdList.get(lastIndex), vmMfList.get(lastIndex));
								}					
							}
							vmsRunningTime = vmsRunningTime + runningTime;
	
						}
	
					} else {
						Log.printLine("A BUG in calculate utilization !!!!!!!!!!!!!!!!!!!!");
					}
	
				}
				

				
				//Calcute all vms total time.
				if(id2VmStartTime.get(vmId) > (CloudSim.clock() - 120)){
					allTime =allTime + ( CloudSim.clock() - id2VmStartTime.get(vmId) );
				} else {
					allTime = allTime + 120;
				}
				

			}

			double utilization = vmsRunningTime / allTime;
			
			if(idleVmIdList.size()==0){

			} else if(utilization >= 0.7) {
				//remove 1 vm
				boolean b = true;
				for(int vmId : id2VmMap.keySet()) {
					if(idleVmIdList.contains(vmId) && b == true) {
						idleVmIdList.remove(Integer.valueOf(vmId));
						sendNow(datacenterId, CloudSimTags.VM_DESTROY_ACK, id2VmMap.get(vmId));
						b = false;
						//return;
					}	
				}
				
			} else if(utilization < 0.7){
				//add 1 vm
				createVm();
			} 
				
			 
			
		}		


	}

	protected double getIntersection(double currentTime, double lastTime, double scheduleTime, double madeFreeTime) {	

		double runningTime = 0.0;
		if (scheduleTime >= lastTime && madeFreeTime >= lastTime) {
			runningTime = madeFreeTime - scheduleTime;
		} else if (scheduleTime <= lastTime && madeFreeTime >= lastTime) {
			runningTime = madeFreeTime - lastTime;
		} else if (scheduleTime >= lastTime && madeFreeTime == -5) {
			runningTime = currentTime - scheduleTime;
		}else if (scheduleTime <= lastTime && madeFreeTime == -5) {
			runningTime = currentTime - lastTime;
		}
		return runningTime;
	}

	@Override
	protected void processCloudletReturn(SimEvent ev) {
		Cloudlet cloudlet = (Cloudlet) ev.getData();
		idleVmIdList.add(cloudlet.getVmId());

		String nodeId = cloudletId2nodeId.get(cloudlet.getCloudletId());
		nodes.get(nodeId).setTaskDone(true);

		Log.printLine(CloudSim.clock()+ ": "+ getName()+ ": Cloudlet "+ cloudlet.getCloudletId()+ " received");
		try {
			cloudlet.setCloudletStatus(Cloudlet.SUCCESS);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		completedTaskCount ++;

		//Utilizations
		ArrayList<Double> list = vmMadeFreeTimes.get(cloudlet.getVmId());
		list.add(CloudSim.clock());
		vmMadeFreeTimes.put(cloudlet.getVmId(), list);
		
		//if all tasks are executed terminate VMs
		if(completedTaskCount==num_tasks) {
			for(int vmId : id2VmMap.keySet()) {
				idleVmIdList.remove(Integer.valueOf(vmId));
				sendNow(datacenterId, CloudSimTags.VM_DESTROY_ACK, id2VmMap.get(vmId));
			}
		}
	}

	protected void createVm() {
		vmId = vmId+1;
		int mips = 10;
		long size = 10000; // image size (MB)
		int ram = 512; // vm memory (MB)
		long bw = 1000;
		int pesNumber = 1; // number of cpus
		String vmm = "Xen"; // VMM name

		// create VM
		Vm vm = new Vm(vmId, getId(), mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
		id2VmMap.put(vmId, vm);

		// send request to datacenter
		Log.printLine(CloudSim.clock() + ": VM # " + vm.getId() + " is requested from datacenter");
		sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
	}

	protected void listPrepareTasks(){

		for (Node node: nodes.values()) {
			
			List<String> parents = node.getParents();
			if(parents == null) {
				addCloudLet(node);
			} else {
				boolean isParentsDone = true;
				for(String parentId : parents) {
					if(!nodes.get(parentId).isTaskDone()){
						isParentsDone = false;
					}
				}
				if(isParentsDone){
					addCloudLet(node);
				}
			}
			
		}
	}

	protected void addCloudLet(Node node) {
		String nodeId = node.getId();
		if(!runningTaskIds.contains(nodeId)) {

			long length = (long) (node.getRuntime()/100);
			cloudletId = cloudletId + 1;
			long fileSize = 300;
			long outputSize = 300;
			int pesNumber = 1;
			UtilizationModel utilizationModel = new UtilizationModelFull();
			Cloudlet cloudlet = new Cloudlet(cloudletId, length, pesNumber, fileSize, outputSize, utilizationModel,
					utilizationModel, utilizationModel);
			cloudlet.setUserId(getId());
			taskList.add(cloudlet);
			runningTaskIds.add(nodeId);
			cloudletId2nodeId.put(cloudletId, nodeId);
			
		}
		
	}

	protected void processVmCreate(SimEvent ev) {
		int[] data = (int[]) ev.getData();
		int datacenterId = data[0];
		int vmId = data[1];
		int result = data[2];

		if (result == CloudSimTags.TRUE) {
			getVmsCreatedList().add(id2VmMap.get(vmId));
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId + " has been created in Datacenter #" +
					datacenterId);
			idleVmIdList.add(vmId);
			id2VmStartTime.put(vmId, CloudSim.clock());
			allVmsId.add(vmId);
			vmScheduledTimes.put(vmId, new ArrayList<Double>());
			vmMadeFreeTimes.put(vmId, new ArrayList<Double>());
		} else {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId +
					" failed in Datacenter #" + datacenterId);
		}
	}

	protected void processVmTerminate(SimEvent ev) {
		int[] data = (int[]) ev.getData();
		int datacenterId = data[0];
		int vmId = data[1];
		int result = data[2];

		if (result == CloudSimTags.TRUE) {
			getVmsCreatedList().add(id2VmMap.get(vmId));
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId +
					" has been terminated in Datacenter #" + datacenterId);
			id2VmMap.remove(vmId);
			//idleVmIdList.remove(Integer.valueOf(vmId));
			id2VmTermiateTime.put(vmId, CloudSim.clock());

		} else {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": Termination of VM #" + vmId +
					" failed in Datacenter #" + datacenterId);
		}
	}


	private void scheduleTasks() {

		if (taskList.size() == 0) {			
			return;
		}

		while (idleVmIdList.size() != 0 && taskList.size() != 0) {
			Cloudlet cloudlet = taskList.remove(0);
			int vmId = idleVmIdList.remove(0);
			ArrayList<Double> list = vmScheduledTimes.get(vmId);
			list.add(CloudSim.clock());
			vmScheduledTimes.put(vmId, list);
			assignTaskOnVm(cloudlet, id2VmMap.get(vmId));
		}

	}

	private void assignTaskOnVm(Cloudlet cloudlet, Vm vm) {
		cloudlet.setVmId(vm.getId());
		cloudlet.setUserId(getId());
		try {
			cloudlet.setCloudletStatus(Cloudlet.INEXEC);

		} catch (Exception e) {
			e.printStackTrace();
		}
		Log.printLine(CloudSim.clock() + ": cloudlet #" + cloudlet.getCloudletId() + "is scheduled on VM #" + vm.getId());
		
		sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
		if(cloudlet.getCloudletId() == num_tasks-1) {
			endLoop = true;
		}
	}

	@Override
	public void shutdownEntity() {
		super.shutdownEntity();
		printResult();
	}

	private void printResult() {
		double vmPrice = 0.001;
		int cost = 0;

		for (int vmId : allVmsId) {
			double startTime = id2VmStartTime.get(vmId);
			double terminateTime = CloudSim.clock();
			if (id2VmTermiateTime.containsKey(vmId)) {
				terminateTime = id2VmTermiateTime.get(vmId);
			}
			double val = (terminateTime - startTime);

			if(val < 600) {
				val = 600;
			}

			cost += val;
		}
		cost *= vmPrice;

		System.out.println("time: " + CloudSim.clock());
		System.out.println("budget: " + cost);

	}

}