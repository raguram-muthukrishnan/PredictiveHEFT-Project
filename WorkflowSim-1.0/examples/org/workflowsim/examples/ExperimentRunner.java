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
import org.workflowsim.CondorVM;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.WorkflowPlanner;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;
import weka.classifiers.Classifier;
import weka.core.SerializationHelper;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import org.workflowsim.Job;

/**
 * Runs two experiments:
 * 1. HEFT (planning) + STATIC (scheduling)
 * 2. Predictive-HEFT (planning) + STATIC (scheduling), if the model file exists
 * <p>
 * Key fixes:
 * - Use WorkflowPlanner constructor WITH DAX list (parses DAG before t=0)
 * - Submit VM list to engine BEFORE CloudSim.startSimulation (avoid race)
 * - Use explicit "no clustering" parameters (avoid ClusteringEngine NPE)
 * - Keep SchedulingAlgorithm = STATIC when using a planning algorithm
 * - Print finished cloudlets and makespan in milliseconds
 */
public class ExperimentRunner {

    // Use Montage_100 for quick tests; switch to Montage_1000 after validation
    private static final String DAX_PATH = "WorkflowSim-1.0/config/dax/Montage_100.xml";
    // Update this to your actual model path for Predictive HEFT
    private static final String MODEL_PATH = "predictive_heft_model.model";
    private static final int VM_NUM = 5;

    public static void main(String[] args) {
        try {
            Log.printLine("====================================================================");
            Log.printLine("    COMPREHENSIVE ALGORITHM COMPARISON EXPERIMENT");
            Log.printLine("    Goal: Compare HEFT vs Predictive HEFT (baseline for future Q-Learning HEFT)");
            Log.printLine("====================================================================");

            // Store results for comparison
            List<ExperimentResult> results = new ArrayList<>();

            Log.printLine("\n>>> EXPERIMENT 1: Standard HEFT Planner <<<");
            ExperimentResult heftResult = runExperiment(Parameters.PlanningAlgorithm.HEFT, null);
            if (heftResult != null) results.add(heftResult);

            Log.printLine("\n>>> EXPERIMENT 2: Predictive HEFT Planner <<<");
            Classifier predictionModel = loadWekaModel(MODEL_PATH);
            if (predictionModel != null) {
                ExperimentResult predictiveResult = runExperiment(Parameters.PlanningAlgorithm.PREDICTIVE_HEFT, predictionModel);
                if (predictiveResult != null) results.add(predictiveResult);
            } else {
                Log.printLine("!ERROR!: Could not load the prediction model. Skipping Predictive HEFT experiment.");
            }

            // Comprehensive comparison analysis
            printComparisonAnalysis(results);

        } catch (Exception e) {
            Log.printLine("The experiment has been terminated due to an unexpected error.");
            e.printStackTrace();
        }
    }

    // Result storage class for comprehensive analysis
    private static class ExperimentResult {
        String algorithmName;
        int finishedJobs;
        double makespanSec;
        long wallTimeMs;
        double totalCost;

        ExperimentResult(String name, int jobs, double makespan, long wallTime, double cost) {
            this.algorithmName = name;
            this.finishedJobs = jobs;
            this.makespanSec = makespan;
            this.wallTimeMs = wallTime;
            this.totalCost = cost;
        }
    }

    private static Classifier loadWekaModel(String modelPath) {
        try {
            File modelFile = new File(modelPath);
            if (!modelFile.exists()) {
                Log.printLine("!FATAL!: Weka model file not found at: " + modelFile.getAbsolutePath());
                return null;
            }
            Log.printLine("Loading Weka model from: " + modelFile.getAbsolutePath());
            Classifier cls = (Classifier) SerializationHelper.read(modelPath);
            Log.printLine("Model loaded successfully.");
            return cls;
        } catch (Exception e) {
            Log.printLine("!FATAL!: Failed to load Weka model from: " + modelPath);
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Critical sequence to prevent race condition:
     * 1. CloudSim.init
     * 2. Parameters.init with explicit "no clustering" object
     * 3. Create datacenter
     * 4. Create VM list
     * 5. Create WorkflowPlanner using DAX list constructor (parses DAG before t=0)
     * 6. Submit VM list to engine
     * 7. Bind datacenter
     * 8. Start simulation
     */
    private static ExperimentResult runExperiment(Parameters.PlanningAlgorithm algorithm, Classifier predictionModel) {
        try {
            // 1) CloudSim init
            CloudSim.init(1, Calendar.getInstance(), false);

            // 2) Validate DAX path
            File daxFile = new File(DAX_PATH);
            if (!daxFile.exists()) {
                Log.printLine("!ERROR!: DAX file not found at " + daxFile.getAbsolutePath());
                return null;
            }

            // 3) WorkflowSim global Parameters
            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);
            // CRITICAL FIX: Force clustering to create individual jobs for each task
            // This ensures all 100 tasks execute individually for proper comparison
            ClusteringParameters cp = new ClusteringParameters(
                    100, 1, ClusteringParameters.ClusteringMethod.NONE, null
            );
            ReplicaCatalog.FileSystem fileSystem = ReplicaCatalog.FileSystem.SHARED;

            // CRITICAL FIX: Use different configurations based on the algorithm
            if (algorithm == Parameters.PlanningAlgorithm.HEFT || algorithm == Parameters.PlanningAlgorithm.PREDICTIVE_HEFT) {
                // For planning algorithms: use INVALID scheduling + planning algorithm
                Log.printLine("Using planning-driven approach: " + algorithm.name() + " planning + INVALID scheduling");
                Parameters.init(
                        VM_NUM, DAX_PATH, null, null, op, cp,
                        Parameters.SchedulingAlgorithm.INVALID,  // Let planner handle assignment
                        algorithm,                               // Use the planning algorithm
                        null, 0
                );
            } else {
                // For baseline comparison: use MINMIN scheduling + INVALID planning
                Log.printLine("Using scheduler-driven approach: MINMIN scheduling + INVALID planning");
                Parameters.init(
                        VM_NUM, DAX_PATH, null, null, op, cp,
                        Parameters.SchedulingAlgorithm.MINMIN,   // Use MINMIN scheduler
                        Parameters.PlanningAlgorithm.INVALID,    // No planning
                        null, 0
                );
            }

            // Attach prediction model if provided (for Predictive HEFT)
            if (predictionModel != null) {
                Parameters.setPredictionModel(predictionModel);
            }
            ReplicaCatalog.init(fileSystem);

            // 4) Infrastructure: datacenter first
            WorkflowDatacenter datacenter0 = createDatacenter("Datacenter_0");

            // 5) Create WorkflowPlanner BEFORE VM creation (like working examples)
            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();

            // 6) Create VM list using scheduler ID (like working examples)
            List<CondorVM> vmList = createVMs(wfEngine.getSchedulerId(0), VM_NUM);
            Log.printLine("VM list prepared with " + vmList.size() + " VMs.");

            // 7) Submit VMs and bind datacenter
            wfEngine.submitVmList(vmList, 0);  // Added scheduler index parameter
            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);

            Log.printLine("Starting simulation for " + algorithm.name() + "...");
            long wallStart = System.nanoTime();
            CloudSim.startSimulation();
            List<Job> finishedList = wfEngine.getJobsReceivedList();
            CloudSim.stopSimulation();
            long wallEnd = System.nanoTime();

            Log.printLine("Simulation finished!");
            printResults(finishedList, algorithm.name(), wallEnd - wallStart);

            // Store result for this experiment
            return storeExperimentResult(finishedList, algorithm.name(), wallEnd - wallStart);

        } catch (Exception e) {
            Log.printLine("An error occurred during the " + algorithm.name() + " simulation run.");
            e.printStackTrace();
            return null;
        }
    }

    private static ExperimentResult storeExperimentResult(List<Job> list, String algorithmName, long simWallNanos) {
        if (list == null || list.isEmpty()) {
            Log.printLine("!ERROR!: No jobs were finished in the simulation for " + algorithmName);
            return null;
        }

        double makespanSec = 0.0;
        double totalCost = 0.0;
        DecimalFormat dft = new DecimalFormat("###.##");

        for (Job job : list) {
            if (job.getFinishTime() > makespanSec) {
                makespanSec = job.getFinishTime();
            }
            totalCost += (job.getCostPerSec() * job.getActualCPUTime());
        }

        long makespanMs = Math.round(makespanSec * 1000.0);
        long wallMs = simWallNanos / 1_000_000L;

        Log.printLine("--------------------------------------------------------------------");
        Log.printLine("                    RESULTS for " + algorithmName);
        Log.printLine("--------------------------------------------------------------------");
        Log.printLine("Finished jobs: " + list.size());
        Log.printLine("Makespan: " + makespanMs + " ms (" + dft.format(makespanSec) + " s)");
        Log.printLine("Scheduling+simulation wall time: " + wallMs + " ms");
        Log.printLine("Total Cost: $" + dft.format(totalCost));
        Log.printLine("--------------------------------------------------------------------");

        // Return stored result object
        return new ExperimentResult(algorithmName, list.size(), makespanSec, wallMs, totalCost);
    }

    private static void printComparisonAnalysis(List<ExperimentResult> results) {
        Log.printLine("====================================================================");
        Log.printLine("                 COMPARISON ANALYSIS: HEFT vs Predictive HEFT");
        Log.printLine("====================================================================");

        // Header
        Log.printLine(String.format("%-20s %-15s %-15s %-20s %-15s", "Algorithm", "Finished Jobs", "Makespan (s)", "Wall Time (ms)", "Total Cost"));

        // Separator
        Log.printLine(String.format("%-20s %-15s %-15s %-20s %-15s", "---------", "-------------", "-------------", "--------------------", "----------"));

        // Data rows
        for (ExperimentResult result : results) {
            Log.printLine(String.format("%-20s %-15d %-15.2f %-20d $%-14.2f",
                    result.algorithmName, result.finishedJobs, result.makespanSec, result.wallTimeMs, result.totalCost));
        }

        Log.printLine("====================================================================");
    }

    private static void printResults(List<Job> list, String algorithmName, long simWallNanos) {
        if (list == null || list.isEmpty()) {
            Log.printLine("!ERROR!: No jobs were finished in the simulation for " + algorithmName);
            return;
        }

        double makespanSec = 0.0;
        double totalCost = 0.0;
        DecimalFormat dft = new DecimalFormat("###.##");

        for (Job job : list) {
            if (job.getFinishTime() > makespanSec) {
                makespanSec = job.getFinishTime();
            }
            totalCost += (job.getCostPerSec() * job.getActualCPUTime());
        }

        long makespanMs = Math.round(makespanSec * 1000.0);
        long wallMs = simWallNanos / 1_000_000L;

        Log.printLine("--------------------------------------------------------------------");
        Log.printLine("                    RESULTS for " + algorithmName);
        Log.printLine("--------------------------------------------------------------------");
        Log.printLine("Finished jobs: " + list.size());
        Log.printLine("Makespan: " + makespanMs + " ms (" + dft.format(makespanSec) + " s)");
        Log.printLine("Scheduling+simulation wall time: " + wallMs + " ms");
        Log.printLine("Total Cost: $" + dft.format(totalCost));
        Log.printLine("--------------------------------------------------------------------");
    }

    // Infrastructure helpers
    protected static WorkflowDatacenter createDatacenter(String name) throws Exception {
        List<Host> hostList = new ArrayList<>();
        int hostNum = Math.max(VM_NUM * 2, 10);
        int pesPerHost = 4;
        int mips = 1000;            // increase if you want faster simulated execution
        int ram = 16384;            // MB
        long storage = 1_000_000;   // MB
        int bw = 10_000;            // bandwidth

        for (int i = 0; i < hostNum; i++) {
            List<Pe> peList = new ArrayList<>();
            for (int j = 0; j < pesPerHost; j++) {
                peList.add(new Pe(j, new PeProvisionerSimple(mips)));
            }
            hostList.add(new Host(
                    i,
                    new RamProvisionerSimple(ram),
                    new BwProvisionerSimple(bw),
                    storage,
                    peList,
                    new VmSchedulerTimeShared(peList)
            ));
        }

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                "x86", "Linux", "Xen", hostList,
                10.0, 3.0, 0.05, 0.001, 0.0);

        return new WorkflowDatacenter(
                name,
                characteristics,
                new VmAllocationPolicySimple(hostList),
                new LinkedList<Storage>(),
                0);
    }

    protected static List<CondorVM> createVMs(int userId, int vms) {
        List<CondorVM> list = new ArrayList<>();
        long size = 10_000;  // image size (MB)
        int ram = 2048;      // MB
        int mips = 1000;     // per PE
        long bw = 1000;
        int pesNumber = 2;   // per VM
        String vmm = "Xen";

        for (int i = 0; i < vms; i++) {
            CondorVM vm = new CondorVM(
                    i, userId, mips, pesNumber, ram, bw, size, vmm,
                    new org.cloudbus.cloudsim.CloudletSchedulerSpaceShared()
            );
            list.add(vm);
        }
        return list;
    }
}
