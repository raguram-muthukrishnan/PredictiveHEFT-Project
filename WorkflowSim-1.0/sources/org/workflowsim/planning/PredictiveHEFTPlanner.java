package org.workflowsim.planning;



import org.cloudbus.cloudsim.Vm;

import org.workflowsim.Job;

import org.workflowsim.Task;

import org.workflowsim.predictive.WekaPredictor;

import org.workflowsim.CondorVM;

import org.cloudbus.cloudsim.Log;

import java.util.*;

/**

 * PredictiveHEFTPlanner extends the HEFT algorithm. Instead of using a simples

 * static calculation for computation cost, it uses a Weka-based machine

 * learning model to predict the execution time of a task on a VM.

 *

 * Enhanced with load balancing: The algorithm now considers both the predicted

 * finish time and the future workload on each VM to make better scheduling decisions.

 */

public class PredictiveHEFTPlanner extends HEFTPlanningAlgorithm {



    // Track the total predicted workload for each VM

    private final Map<CondorVM, Double> vmWorkloads;



    // Internal data structures for our enhanced algorithm

    private Map<Task, Map<CondorVM, Double>> computationCosts;

    private Map<Task, Map<Task, Double>> transferCosts;

    private Map<Task, Double> rank;

    private Map<CondorVM, List<Event>> schedules;

    private Map<Task, Double> earliestFinishTimes;

    private double averageBandwidth;



    // Weighting factors for the scoring function

    private static final double FINISH_TIME_WEIGHT = 0.7;

    private static final double WORKLOAD_WEIGHT = 0.3;



    // Internal Event class for scheduling

    private class Event {

        public double start;

        public double finish;



        public Event(double start, double finish) {

            this.start = start;

            this.finish = finish;

        }

    }



    // Internal TaskRank class for prioritization

    private class TaskRank implements Comparable<TaskRank> {

        public Task task;

        public Double rank;



        public TaskRank(Task task, Double rank) {

            this.task = task;

            this.rank = rank;

        }



        @Override

        public int compareTo(TaskRank o) {

            return o.rank.compareTo(rank);

        }

    }



    public PredictiveHEFTPlanner() {

        super();

        vmWorkloads = new HashMap<>();

        computationCosts = new HashMap<>();

        transferCosts = new HashMap<>();

        rank = new HashMap<>();

        earliestFinishTimes = new HashMap<>();

        schedules = new HashMap<>();

    }



    /**

     * Override the run method to implement our enhanced load-balancing algorithm

     */

    @Override

    public void run() {

        Log.printLine("Enhanced Predictive HEFT planner with load balancing running with " +
                     getTaskList().size() + " tasks.");

        // Initialize data structures

        averageBandwidth = calculateAverageBandwidth();



        for (Object vmObject : getVmList()) {

            CondorVM vm = (CondorVM) vmObject;

            schedules.put(vm, new ArrayList<>());

            vmWorkloads.put(vm, 0.0);

        }



        // Prioritization phase

        calculateComputationCosts();

        calculateTransferCosts();

        calculateRanks();



        // Enhanced selection phase with load balancing

        allocateTasksWithLoadBalancing();

    }



    /**

     * Calculate average bandwidth among all VMs

     */

    private double calculateAverageBandwidth() {

        double avg = 0.0;

        for (Object vmObject : getVmList()) {

            CondorVM vm = (CondorVM) vmObject;

            avg += vm.getBw();

        }

        return avg / getVmList().size();

    }



    /**

     * Calculate computation costs using our predictive model

     */

    private void calculateComputationCosts() {

        for (Task task : getTaskList()) {

            Map<CondorVM, Double> costsVm = new HashMap<>();

            for (Object vmObject : getVmList()) {

                CondorVM vm = (CondorVM) vmObject;

                if (vm.getNumberOfPes() < task.getNumberOfPes()) {

                    costsVm.put(vm, Double.MAX_VALUE);

                } else {

                    // Use our predictive model

                    double predictedCost = getComputationCost(task, vm);

                    costsVm.put(vm, predictedCost);

                }

            }

            computationCosts.put(task, costsVm);

        }

    }



    /**

     * Calculate transfer costs between tasks

     */

    private void calculateTransferCosts() {

        // Initialize the matrix

        for (Task task1 : getTaskList()) {

            Map<Task, Double> taskTransferCosts = new HashMap<>();

            for (Task task2 : getTaskList()) {

                taskTransferCosts.put(task2, 0.0);

            }

            transferCosts.put(task1, taskTransferCosts);

        }



        // Populate with actual transfer costs

        for (Task parent : getTaskList()) {

            for (Task child : parent.getChildList()) {

                transferCosts.get(parent).put(child, calculateTransferCost(parent, child));

            }

        }

    }



    /**

     * Calculate transfer cost between two tasks

     */

    private double calculateTransferCost(Task parent, Task child) {

        List<org.workflowsim.FileItem> parentFiles = parent.getFileList();

        List<org.workflowsim.FileItem> childFiles = child.getFileList();



        double transferCost = 0.0;

        for (org.workflowsim.FileItem parentFile : parentFiles) {

            if (parentFile.getType() == org.workflowsim.utils.Parameters.FileType.OUTPUT) {

                for (org.workflowsim.FileItem childFile : childFiles) {

                    if (childFile.getType() == org.workflowsim.utils.Parameters.FileType.INPUT
                        && childFile.getName().equals(parentFile.getName())) {

                        transferCost += parentFile.getSize();

                        break;

                    }

                }

            }

        }

        // Convert to time: size / bandwidth

        return transferCost / averageBandwidth;

    }



    /**

     * Calculate ranks for all tasks (upward rank)

     */

    private void calculateRanks() {

        for (Task task : getTaskList()) {

            calculateRank(task);

        }

    }



    /**

     * Calculate rank for a specific task

     */

    private double calculateRank(Task task) {

        if (rank.containsKey(task)) {

            return rank.get(task);

        }



        double averageComputationCost = 0.0;

        for (Object vmObject : getVmList()) {

            CondorVM vm = (CondorVM) vmObject;

            averageComputationCost += computationCosts.get(task).get(vm);

        }

        averageComputationCost /= getVmList().size();



        double maxChildRank = 0.0;

        for (Task child : task.getChildList()) {

            double childRank = calculateRank(child);

            double transferCost = transferCosts.get(task).get(child);

            maxChildRank = Math.max(maxChildRank, transferCost + childRank);

        }



        double taskRank = averageComputationCost + maxChildRank;

        rank.put(task, taskRank);

        return taskRank;

    }



    /**

     * Enhanced task allocation with load balancing

     */

    private void allocateTasksWithLoadBalancing() {

        List<TaskRank> taskRank = new ArrayList<>();

        for (Task task : rank.keySet()) {

            taskRank.add(new TaskRank(task, rank.get(task)));

        }



        // Sort in non-ascending order of rank

        Collections.sort(taskRank);


        for (TaskRank tRank : taskRank) {

            allocateTaskWithLoadBalancing(tRank.task);

        }

    }



    /**

     * Allocate a single task with load balancing consideration

     */

    private void allocateTaskWithLoadBalancing(Task task) {

        CondorVM chosenVM = null;

        double bestScore = Double.MAX_VALUE;

        double bestReadyTime = 0.0;

        double bestFinishTime = 0.0;



        System.out.println("[PREDICTIVE-HEFT] Load-balancing allocation for task " + task.getCloudletId());

        for (Object vmObject : getVmList()) {

            CondorVM vm = (CondorVM) vmObject;

            double minReadyTime = 0.0;

            // Calculate the minimum ready time based on parent task completion

            for (Task parent : task.getParentList()) {

                double readyTime = earliestFinishTimes.get(parent);

                if (parent.getVmId() != vm.getId()) {

                    readyTime += transferCosts.get(parent).get(task);

                }

                minReadyTime = Math.max(minReadyTime, readyTime);

            }

            // Get the predicted finish time for this task on this VM

            double finishTime = findFinishTime(task, vm, minReadyTime, false);



            // Get the current workload on this VM

            double currentWorkload = vmWorkloads.get(vm);



            // Calculate the predicted execution time for this task

            double executionTime = computationCosts.get(task).get(vm);



            // Calculate a composite score that considers both finish time and workload

            double score = FINISH_TIME_WEIGHT * finishTime + WORKLOAD_WEIGHT * currentWorkload;



            System.out.println("[PREDICTIVE-HEFT] VM " + vm.getId() +

                             " - Finish Time: " + finishTime +

                             ", Current Workload: " + currentWorkload +

                             ", Execution Time: " + executionTime +

                             ", Score: " + score);

            if (score < bestScore) {

                bestScore = score;

                bestReadyTime = minReadyTime;

                bestFinishTime = finishTime;

                chosenVM = vm;

            }

        }

        // Schedule the task on the chosen VM

        findFinishTime(task, chosenVM, bestReadyTime, true);

        earliestFinishTimes.put(task, bestFinishTime);



        // Update the workload tracking for the chosen VM

        double taskExecutionTime = computationCosts.get(task).get(chosenVM);

        vmWorkloads.put(chosenVM, vmWorkloads.get(chosenVM) + taskExecutionTime);



        task.setVmId(chosenVM.getId());



        System.out.println("[PREDICTIVE-HEFT] Task " + task.getCloudletId() +
                         " allocated to VM " + chosenVM.getId() +
                         " with score " + bestScore +
                         ". VM workload is now: " + vmWorkloads.get(chosenVM));
    }



    /**

     * Find the best time slot for a task on a VM

     */

    private double findFinishTime(Task task, CondorVM vm, double readyTime, boolean occupySlot) {

        List<Event> sched = schedules.get(vm);

        double computationCost = computationCosts.get(task).get(vm);

        double start, finish;

        int pos;



        if (sched.isEmpty()) {

            if (occupySlot) {

                sched.add(new Event(readyTime, readyTime + computationCost));

            }

            return readyTime + computationCost;

        }



        if (sched.size() == 1) {

            if (readyTime >= sched.get(0).finish) {

                pos = 1;

                start = readyTime;

            } else if (readyTime + computationCost <= sched.get(0).start) {

                pos = 0;

                start = readyTime;

            } else {

                pos = 1;

                start = sched.get(0).finish;

            }



            if (occupySlot) {

                sched.add(pos, new Event(start, start + computationCost));

            }

            return start + computationCost;

        }



        // Trivial case: Start after the latest task scheduled

        start = Math.max(readyTime, sched.get(sched.size() - 1).finish);

        finish = start + computationCost;

        int i = sched.size() - 1;

        int j = sched.size() - 2;

        pos = i + 1;

        while (j >= 0) {
            Event current = sched.get(i);
            Event previous = sched.get(j);

            if (readyTime > previous.finish) {
                if (readyTime + computationCost <= current.start) {
                    start = readyTime;
                    finish = readyTime + computationCost;
                    pos = i;
                }
                break;
            }
            if (previous.finish + computationCost <= current.start) {
                start = previous.finish;
                finish = previous.finish + computationCost;
                pos = i;
            }
            i--;
            j--;
        }

        if (j < 0) {
            if (readyTime + computationCost <= sched.get(0).start) {
                pos = 0;
                start = readyTime;
                finish = readyTime + computationCost;
            }
        }

        if (occupySlot) {
            sched.add(pos, new Event(start, finish));
        }



        return finish;

    }



    /**

     * Overrides the default computation cost calculation using ML prediction

     */

    protected double getComputationCost(Task task, Vm vm) {

        System.out.println("[PREDICTIVE-HEFT] getComputationCost called for task " + task.getCloudletId() + " on VM " + vm.getId());



        if (task instanceof Job) {

            double predictedTime = WekaPredictor.predictExecutionTime((Job) task, vm);

            System.out.println("[PREDICTIVE-HEFT] ML model predicted execution time: " + predictedTime + " for task " + task.getCloudletId());

            return predictedTime;

        } else {

            System.out.println("[PREDICTIVE-HEFT] Falling back to standard HEFT for non-Job task " + task.getCloudletId());

            return super.getComputationCost(task, vm);

        }

    }

}
