package org.workflowsim.examples;



import org.cloudbus.cloudsim.Cloudlet;

import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;

import org.cloudbus.cloudsim.DatacenterCharacteristics;

import org.cloudbus.cloudsim.Host;

import org.cloudbus.cloudsim.Log;

import org.cloudbus.cloudsim.Pe;

import org.cloudbus.cloudsim.Vm;

import org.cloudbus.cloudsim.VmAllocationPolicySimple;

import org.cloudbus.cloudsim.VmSchedulerTimeShared;

import org.cloudbus.cloudsim.core.CloudSim;

// Corrected Imports

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

 * This TrainingDataGenerator class is a modified version of an example

 * to generate a training dataset in ARFF format for Weka. It runs a simulation

 * with a large workflow and logs the performance of each completed task.

 */

public class TrainingDataGenerator {



    /**

     * The list of created VMs.

     */

    private static List<CondorVM> vmList;

    /**

     * The list of cloudlets that have finished executing.

     */

    private static List<Cloudlet> finishedList;



    /**

     * Creates main() to run this example.

     * This example has only one datacenter and one user.

     */

    public static void main(String[] args) {



        try {

// First, run the simulation to get the results

            runSimulation();



// Second, process the results to generate the ARFF file

            if (finishedList != null && !finishedList.isEmpty()) {

                generateArffFile(finishedList);

            } else {

                Log.printLine("Simulation did not produce any finished cloudlets. ARFF file not generated.");

            }





        } catch (Exception e) {

            Log.printLine("The simulation has been terminated due to an unexpected error");

            e.printStackTrace();

        }

    }



    /**

     * Runs the simulation and populates the finishedList.

     */

    private static void runSimulation() {

        try {

// First, we need to initialize CloudSim

            int num_user = 1; // number of grid users

            Calendar calendar = Calendar.getInstance();

            boolean trace_flag = false; // mean trace events



// Initialize the CloudSim library

            CloudSim.init(num_user, calendar, trace_flag);



// Specify the workflow details

// Make sure the path is correct for your project structure

            String daxPath = "WorkflowSim-1.0/config/dax/Montage_1000.xml";

            File daxFile = new File(daxPath);

            if (!daxFile.exists()) {

                Log.printLine("Warning: Please replace daxPath with the correct physical path to your DAX file.");

                Log.printLine("Current path: " + daxFile.getAbsolutePath());

                return;

            }



            /*

             * Configure the simulation parameters

             */

            Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.STATIC;

            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.HEFT;

            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;



            /*

             * No overheads

             */

            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);



            /*

             * No clustering

             */

            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;

            ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);



            /*

             * Initialize the simulation parameters

             */

            Parameters.init(20, daxPath, null,

                    null, op, cp, sch_method, pln_method,

                    null, 0);

            ReplicaCatalog.init(file_system);



// Create a Datacenter

            WorkflowDatacenter datacenter0 = createDatacenter("Datacenter_0");



// Create a WorkflowPlanner

            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);

// Create a WorkflowEngine

            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();

// Create a list of VMs and assign it to our static list

            vmList = createVM(wfEngine.getSchedulerId(0), Parameters.getVmNum());

// Set the VM list for the WorkflowEngine

            wfEngine.submitVmList(vmList, 0);

// Bind the workflow to the WorkflowEngine

            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);



            CloudSim.startSimulation();

// --- FIX: The correct method in this version is getJobsReceivedList()

            finishedList = wfEngine.getJobsReceivedList();

            CloudSim.stopSimulation();



            Log.printLine("Simulation finished!");



        } catch (Exception e) {

            Log.printLine("An error occurred during simulation: " + e.getMessage());

            e.printStackTrace();

        }

    }



    /**

     * Generates the ARFF file from the list of finished cloudlets.

     * @param list The list of finished cloudlets.

     */

    private static void generateArffFile(List<Cloudlet> list) {

// The output file will be created in the root of the project

        try (FileWriter writer = new FileWriter("training_data.arff")) {

// Write the ARFF header

            writer.write("@RELATION task_execution_prediction\n\n");

            writer.write("@ATTRIBUTE task_length NUMERIC\n");

            writer.write("@ATTRIBUTE num_parents NUMERIC\n");

            writer.write("@ATTRIBUTE vm_mips NUMERIC\n");

            writer.write("@ATTRIBUTE vm_pes NUMERIC\n");

            writer.write("@ATTRIBUTE actual_execution_time NUMERIC\n\n");

            writer.write("@DATA\n");



            DecimalFormat dft = new DecimalFormat("###.##");



            for (Cloudlet cloudlet : list) {

// We only want to log valid tasks that have finished successfully

                if (cloudlet.getCloudletStatus() != Cloudlet.SUCCESS) {

                    continue;

                }



                Job job = (Job) cloudlet;

                String taskLength = dft.format(job.getCloudletLength());

                String numParents = Integer.toString(job.getParentList().size());



// --- FIX: Find the Vm object using the vmId from the cloudlet

                int vmId = cloudlet.getVmId();

                Vm vm = null;

                for (Vm v : vmList) {

                    if (v.getId() == vmId) {

                        vm = v;

                        break;

                    }

                }



                if (vm == null) {

                    Log.printLine("Warning: Could not find VM with ID " + vmId + " for Cloudlet " + cloudlet.getCloudletId());

                    continue;

                }



                String vmMips = dft.format(vm.getMips());

                String vmPes = Integer.toString(vm.getNumberOfPes());

// The actual execution time is the finish time minus the start time

                String actualTime = dft.format(job.getFinishTime() - job.getExecStartTime());



                writer.write(taskLength + "," +

                        numParents + "," +

                        vmMips + "," +

                        vmPes + "," +

                        actualTime + "\n");

            }

            Log.printLine("Successfully created training_data.arff file.");



        } catch (Exception e) {

            Log.printLine("Error creating ARFF file: " + e.getMessage());

            e.printStackTrace();

        }

    }



    /**

     * Creates the datacenter.

     * @param name The name of the datacenter.

     * @return The created WorkflowDatacenter.

     */

    protected static WorkflowDatacenter createDatacenter(String name) {

        List<Host> hostList = new ArrayList<>();

        int mips = 1000;

        int ram = 2048;

        long storage = 1000000;

        int bw = 10000;

        for (int i = 0; i < Parameters.getVmNum(); i++) {

            List<Pe> peList = new ArrayList<>();

            peList.add(new Pe(0, new PeProvisionerSimple(mips)));

            hostList.add(new Host(i, new RamProvisionerSimple(ram),

                    new BwProvisionerSimple(bw), storage,

                    peList,

                    new VmSchedulerTimeShared(peList)));

        }

        String arch = "x86";

        String os = "Linux";

        String vmm = "Xen";

        double time_zone = 10.0;

        double cost = 3.0;

        double costPerMem = 0.05;

        double costPerStorage = 0.001;

        double costPerBw = 0.0;

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(

                arch, os, vmm, hostList, time_zone, cost, costPerMem,

                costPerStorage, costPerBw);

        WorkflowDatacenter datacenter = null;

        try {

            datacenter = new WorkflowDatacenter(name, characteristics,

                    new VmAllocationPolicySimple(hostList), new LinkedList<>(), 0);

        } catch (Exception e) {

            e.printStackTrace();

        }

        return datacenter;

    }



    /**

     * Creates a list of VMs.

     * @param userId The user ID.

     * @param vms The number of VMs.

     * @return A list of CondorVMs.

     */

    protected static List<CondorVM> createVM(int userId, int vms) {

        LinkedList<CondorVM> list = new LinkedList<>();

        long size = 10000;

        int ram = 512;

        int mips = 1000;

        long bw = 1000;

        int pesNumber = 1;

        String vmm = "Xen";

        for (int i = 0; i < vms; i++) {

            double ratio = 1.0;

            CondorVM vm = new CondorVM(i, userId, mips * ratio, pesNumber, ram,

                    bw, size, vmm, new CloudletSchedulerSpaceShared());

            list.add(vm);

        }

        return list;

    }

}