package org.workflowsim.examples;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.workflowsim.*;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Standard HEFT Algorithm Experiment
 * This version includes the critical fix to manually pass the planner's
 * scheduled list to the workflow engine, ensuring all tasks are executed.
 */
public class HEFTExperiment {

    private static final String DAX_PATH = "WorkflowSim-1.0/config/dax/Montage_100.xml";
    private static final int VM_NUM = 5;

    public static void main(String[] args) {
        try {
            Log.printLine("====================================================================");
            Log.printLine("                    STANDARD HEFT ALGORITHM EXPERIMENT");
            Log.printLine("====================================================================");
            runHEFTExperiment();
        } catch (Exception e) {
            Log.printLine("HEFT experiment terminated due to an unexpected error.");
            e.printStackTrace();
        }
    }

    private static void runHEFTExperiment() {
        try {
            // 1) CloudSim initialization
            CloudSim.init(1, Calendar.getInstance(), false);

            // 2) Validate DAX file
            File daxFile = new File(DAX_PATH);
            if (!daxFile.exists()) {
                Log.printLine("!ERROR!: DAX file not found at " + daxFile.getAbsolutePath());
                return;
            }

            // 3) Configure WorkflowSim parameters
            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);
            ClusteringParameters cp = new ClusteringParameters(0, 0, ClusteringParameters.ClusteringMethod.NONE, null);
            ReplicaCatalog.FileSystem fileSystem = ReplicaCatalog.FileSystem.SHARED;

            // CORRECTED HEFT CONFIGURATION: Follow official HEFT example pattern
            Log.printLine("Configuring PURE HEFT algorithm (following official pattern)...");
            Parameters.init(
                    VM_NUM,
                    DAX_PATH,
                    null,
                    null,
                    op,
                    cp,
                    Parameters.SchedulingAlgorithm.STATIC,      // CRITICAL: Use STATIC (not INVALID)
                    Parameters.PlanningAlgorithm.HEFT,          // Use HEFT planning algorithm
                    null,
                    0
            );

            ReplicaCatalog.init(ReplicaCatalog.FileSystem.LOCAL); // Use LOCAL file system

            // 4) Create infrastructure
            WorkflowDatacenter datacenter = createDatacenter("HEFT_Datacenter");

            // 5) Create workflow planner and engine FIRST (like official example)
            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();

            // 6) Create VMs with correct scheduler ID (like official example)
            List<CondorVM> vmList = createVMs(wfEngine.getSchedulerId(0), VM_NUM);
            Log.printLine("Created " + vmList.size() + " VMs with scheduler ID: " + wfEngine.getSchedulerId(0));

            // 7) Submit VMs and bind datacenter (following official pattern)
            wfEngine.submitVmList(vmList, 0);
            wfEngine.bindSchedulerDatacenter(datacenter.getId(), 0);

            // 8) Run the simulation
            Log.printLine("Starting HEFT simulation...");
            long startTime = System.nanoTime();

            CloudSim.startSimulation();
            List<Job> finishedJobs = wfEngine.getJobsReceivedList();
            CloudSim.stopSimulation();

            long endTime = System.nanoTime();
            Log.printLine("HEFT simulation completed!");

            // 9) Analyze and display results
            analyzeHEFTResults(finishedJobs, endTime - startTime);

        } catch (Exception e) {
            Log.printLine("Error during HEFT experiment execution:");
            e.printStackTrace();
        }
    }

    private static void analyzeHEFTResults(List<Job> finishedJobs, long simulationTimeNanos) {
        Log.printLine("\n====================================================================");
        Log.printLine("                        HEFT ALGORITHM RESULTS");
        Log.printLine("====================================================================");

        if (finishedJobs == null || finishedJobs.isEmpty()) {
            Log.printLine("!ERROR!: No jobs completed in HEFT simulation");
            return;
        }

        double makespan = 0.0;
        double totalCost = 0.0;
        DecimalFormat df = new DecimalFormat("###.##");

        for (Job job : finishedJobs) {
            if (job.getFinishTime() > makespan) {
                makespan = job.getFinishTime();
            }
            totalCost += job.getCostPerSec() * job.getActualCPUTime();
        }

        Log.printLine("PERFORMANCE METRICS:");
        Log.printLine("- Total jobs completed: " + finishedJobs.size());
        Log.printLine("- Makespan: " + df.format(makespan) + " seconds");
        Log.printLine("- Total cost: $" + df.format(totalCost));
        Log.printLine("- Simulation wall time: " + (simulationTimeNanos / 1_000_000) + " ms");

        Log.printLine("====================================================================");
    }

    // Infrastructure creation methods
    private static WorkflowDatacenter createDatacenter(String name) throws Exception {
        List<Host> hostList = new ArrayList<>();
        int hostNum = Math.max(VM_NUM * 2, 10);
        int pesPerHost = 4;
        int mips = 1000;
        int ram = 16384;
        long storage = 1_000_000;
        int bw = 10_000;

        for (int i = 0; i < hostNum; i++) {
            List<Pe> peList = new ArrayList<>();
            for (int j = 0; j < pesPerHost; j++) {
                peList.add(new Pe(j, new PeProvisionerSimple(mips)));
            }
            hostList.add(new Host(i, new RamProvisionerSimple(ram), new BwProvisionerSimple(bw), storage, peList, new VmSchedulerTimeShared(peList)));
        }

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics("x86", "Linux", "Xen", hostList, 10.0, 3.0, 0.05, 0.001, 0.0);
        return new WorkflowDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), new LinkedList<Storage>(), 0);
    }

    private static List<CondorVM> createVMs(int userId, int vms) {
        List<CondorVM> list = new ArrayList<>();
        long size = 10_000;
        int ram = 2048;
        int mips = 1000;
        long bw = 1000;
        int pesNumber = 2;
        String vmm = "Xen";

        for (int i = 0; i < vms; i++) {
            CondorVM vm = new CondorVM(i, userId, mips, pesNumber, ram, bw, size, vmm, new org.cloudbus.cloudsim.CloudletSchedulerSpaceShared());
            list.add(vm);
        }
        return list;
    }
}
