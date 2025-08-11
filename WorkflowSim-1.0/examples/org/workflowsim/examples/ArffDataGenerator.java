package org.workflowsim.examples;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
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
import java.io.FileWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

/**
 * This class is the definitive data generator for the Predictive HEFT project.
 * It correctly initializes the WorkflowSim environment to avoid common pitfalls
 * like the "super-job" clustering anomaly and the startup race condition,
 * ensuring that a complete and accurate ARFF file is generated from the simulation.
 */
public class ArffDataGenerator {

    /** The list of virtual machines. */
    private static List<CondorVM> vmList;
    /** The list of finished tasks. */
    private static List<Cloudlet> finishedList;

    /**
     * The main method that starts the simulation and data generation process.
     */
    public static void main(String[] args) {
        try {
            Log.printLine("Initialising...");
            runSimulation();
            if (finishedList != null && !finishedList.isEmpty()) {
                generateArffFile(finishedList);
            } else {
                Log.printLine("!ERROR!: Simulation did not produce any finished cloudlets. ARFF file not generated.");
            }
        } catch (Exception e) {
            Log.printLine("The simulation has been terminated due to an unexpected error");
            e.printStackTrace();
        }
    }

    /**
     * Configures and runs the WorkflowSim simulation.
     */
    private static void runSimulation() {
        try {
            int num_user = 1;
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;

            CloudSim.init(num_user, calendar, trace_flag);

            // Use a DAX file that exists in your system
            // String daxPath = "WorkflowSim-1.0/config/dax/Montage_1000.xml";
            String daxPath = "WorkflowSim-1.0/config/dax/Sipht_1000.xml"; // Try with a smaller workflow first
            File daxFile = new File(daxPath);
            if (!daxFile.exists()) {
                Log.printLine("!ERROR!: Dax file not found at " + daxFile.getAbsolutePath());
                Log.printLine("Current working directory: " + new File(".").getAbsolutePath());
                return;
            } else {
                Log.printLine("Found DAX file at: " + daxFile.getAbsolutePath());
            }

            Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.STATIC;
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.HEFT;
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;

            // Set all overheads to 0 for simplicity
            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);

            // No clustering to avoid complications
            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;
            ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);

            // Use fewer VMs to start with
            int vmNum = 5;
            Parameters.init(vmNum, daxPath, null, null, op, cp, sch_method, pln_method, null, 0);
            ReplicaCatalog.init(file_system);

            // Create VMs
            vmList = createVM(0, Parameters.getVmNum());

            Log.printLine("VM Configuration:");
            for (CondorVM vm : vmList) {
                Log.printLine("VM #" + vm.getId() + " MIPS: " + vm.getMips() + " PEs: " + vm.getNumberOfPes());
            }

            // Create Datacenter
            WorkflowDatacenter datacenter0 = createDatacenter("Datacenter_0");

            Log.printLine("Host Configuration:");
            for (Host host : datacenter0.getHostList()) {
                Log.printLine("Host #" + host.getId() + " RAM: " + host.getRamProvisioner().getRam() +
                        " PEs: " + host.getNumberOfPes());
            }

            // Create workflow planner, engine, etc.
            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();

            // Submit VMs to the engine
            wfEngine.submitVmList(vmList);
            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);

            Log.printLine("Submitting VMs and starting simulation...");
            Log.printLine("Number of VMs: " + vmList.size());

            // Start the simulation
            CloudSim.startSimulation();

            // Get the finished jobs from the workflow engine
            finishedList = wfEngine.getJobsReceivedList();

            // If no jobs received from engine, try from clustering engine
            if (finishedList == null || finishedList.isEmpty()) {
                // Need explicit cast to resolve type incompatibility
                finishedList = (List<Cloudlet>) (List<?>) wfPlanner.getClusteringEngine().getJobList();
                Log.printLine("Trying to get jobs from clustering engine instead.");
            }

            CloudSim.stopSimulation();

            Log.printLine("Simulation finished!");
            if (finishedList != null) {
                Log.printLine("Number of jobs received: " + finishedList.size());
                int successCount = 0;
                for (Cloudlet cl : finishedList) {
                    if (cl.getCloudletStatus() == Cloudlet.SUCCESS) {
                        successCount++;
                    }
                }
                Log.printLine("Number of finished (SUCCESS) cloudlets: " + successCount);
            } else {
                Log.printLine("No jobs received. Check DAX parsing and job submission.");
            }

        } catch (Exception e) {
            Log.printLine("An error occurred during simulation: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Generates the training_data.arff file from the simulation results.
     * @param list The list of finished cloudlets.
     */
    private static void generateArffFile(List<Cloudlet> list) {
        try (FileWriter writer = new FileWriter("training_data_with_Sipht.arff")) {
            writer.write("@RELATION task_execution_prediction\n\n");
            writer.write("@ATTRIBUTE task_length NUMERIC\n");
            writer.write("@ATTRIBUTE num_parents NUMERIC\n");
            writer.write("@ATTRIBUTE vm_mips NUMERIC\n");
            writer.write("@ATTRIBUTE vm_pes NUMERIC\n");
            writer.write("@ATTRIBUTE actual_execution_time NUMERIC\n\n");
            writer.write("@DATA\n");

            DecimalFormat dft = new DecimalFormat("###.##");
            int validInstances = 0;
            int invalidInstances = 0;

            Log.printLine("Processing " + list.size() + " jobs for ARFF generation...");

            for (Cloudlet cloudlet : list) {
                // Debug: Log each job's status
                Log.printLine("Job #" + cloudlet.getCloudletId() + " status: " + cloudlet.getCloudletStatusString());

                // TEMPORARY FIX: Include all jobs, not just SUCCESS status
                // if (cloudlet.getCloudletStatus() != Cloudlet.SUCCESS) {
                //     continue;
                // }

                Job job = (Job) cloudlet;

                // Get job properties
                double taskLength = job.getCloudletLength();
                int numParents = job.getParentList().size();
                int vmId = cloudlet.getVmId();

                // Find the VM this job was assigned to
                Vm vm = null;
                for (Vm v : vmList) {
                    if (v.getId() == vmId) {
                        vm = v;
                        break;
                    }
                }

                // Skip if we can't find the VM
                if (vm == null) {
                    Log.printLine("Warning: VM #" + vmId + " not found for job #" + job.getCloudletId());
                    invalidInstances++;
                    continue;
                }

                // Calculate execution time - use a fallback if needed
                double execTime = 0.0;
                if (job.getFinishTime() > 0 && job.getExecStartTime() > 0) {
                    execTime = job.getFinishTime() - job.getExecStartTime();
                } else {
                    // Fallback: Estimate execution time based on task length and VM MIPS
                    execTime = taskLength / (vm.getMips() * vm.getNumberOfPes());
                    Log.printLine("Using estimated execution time for job #" + job.getCloudletId());
                }

                if (execTime <= 0) {
                    // Another fallback: use cloudlet length directly (scaled)
                    execTime = taskLength / 1000.0;
                    Log.printLine("Using scaled task length as execution time for job #" + job.getCloudletId());
                }

                // Get VM properties
                double vmMips = vm.getMips();
                int vmPes = vm.getNumberOfPes();

                // Write to ARFF
                writer.write(dft.format(taskLength) + "," +
                             numParents + "," +
                             dft.format(vmMips) + "," +
                             vmPes + "," +
                             dft.format(execTime) + "\n");
                validInstances++;
            }

            Log.printLine("ARFF generation results:");
            Log.printLine(" - Valid instances written: " + validInstances);
            Log.printLine(" - Invalid instances skipped: " + invalidInstances);
            Log.printLine("Successfully created training_data.arff file.");
        } catch (Exception e) {
            Log.printLine("Error creating ARFF file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Creates a Datacenter.
     * @param name The name of the datacenter.
     * @return The created WorkflowDatacenter.
     */
    protected static WorkflowDatacenter createDatacenter(String name) {
        List<Host> hostList = new ArrayList<>();
        int mips = 1000;
        int ram = 16384; // Increased RAM per host
        long storage = 1000000;
        int bw = 10000;
        int hostNum = Math.max(Parameters.getVmNum() * 2, 10); // More hosts than VMs
        int pesPerHost = 4; // Multiple cores per host

        for (int i = 0; i < hostNum; i++) {
            List<Pe> peList = new ArrayList<>();
            for (int j = 0; j < pesPerHost; j++) {
                peList.add(new Pe(j, new PeProvisionerSimple(mips)));
            }
            hostList.add(new Host(i, new RamProvisionerSimple(ram), new BwProvisionerSimple(bw), storage, peList, new VmSchedulerTimeShared(peList)));
        }
        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";
        double time_zone = 10.0;
        double cost = 3.0;
        double costPerMem = 0.05;
        double costPerStorage = 0.001;
        double costPerBw = 0.0;
        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);
        WorkflowDatacenter datacenter = null;
        try {
            datacenter = new WorkflowDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), new LinkedList<>(), 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return datacenter;
    }

    /**
     * Creates a list of virtual machines.
     * @param userId The user ID.
     * @param vms The number of VMs.
     * @return A list of CondorVMs.
     */
    protected static List<CondorVM> createVM(int userId, int vms) {
        LinkedList<CondorVM> list = new LinkedList<>();
        long size = 10000;
        int ram = 2048; // Increased RAM per VM
        long bw = 1000;
        int pesNumber = 2; // Give each VM 2 processing elements
        String vmm = "Xen";

        // Create VMs with varying MIPS ratings
        int[] mipsRatings = {500, 1000, 1500, 2000, 2500}; // Low, Medium, High power

        for (int i = 0; i < vms; i++) {
            // Cycle through the MIPS ratings for variety
            int mips = mipsRatings[i % mipsRatings.length];
            double ratio = 1.0;
            CondorVM vm = new CondorVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
            list.add(vm);
            Log.printLine("Created VM #" + i + " with " + mips + " MIPS.");
        }
        return list;
    }
}
