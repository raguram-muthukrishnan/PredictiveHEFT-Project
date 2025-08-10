package org.workflowsim.planning;



import org.cloudbus.cloudsim.Vm;

import org.workflowsim.Job;

import org.workflowsim.Task;

import org.workflowsim.predictive.WekaPredictor;



/**

 * PredictiveHEFTPlanner extends the HEFT algorithm. Instead of using a simple

 * static calculation for computation cost, it uses a Weka-based machine

 * learning model to predict the execution time of a task on a VM.

 */

public class PredictiveHEFTPlanner extends HEFTPlanningAlgorithm {



    public PredictiveHEFTPlanner() {

        super();

    }



    /**

     * Overrides the default computation cost calculation. This method is the core

     * of the predictive planner. It calls the WekaPredictor to get a predicted

     * execution time.

     *

     * @param task The task for which to calculate the cost.

     * @param vm The VM on which the task would be scheduled.

     * @return The predicted computation time in seconds.

     */

    protected double getComputationCost(Task task, Vm vm) {

        // Add debugging to verify this method is being called

        System.out.println("[PREDICTIVE-HEFT] getComputationCost called for task " + task.getCloudletId() + " on VM " + vm.getId());



        // We must ensure the task is a Job object to access parent lists etc.

        if (task instanceof Job) {

            double predictedTime = WekaPredictor.predictExecutionTime((Job) task, vm);

            System.out.println("[PREDICTIVE-HEFT] ML model predicted execution time: " + predictedTime + " for task " + task.getCloudletId());

            return predictedTime;

        } else {

            // Fallback for non-job tasks, though in WorkflowSim they are typically Jobs.

            System.out.println("[PREDICTIVE-HEFT] Falling back to standard HEFT for non-Job task " + task.getCloudletId());

            return super.getComputationCost(task, vm);

        }

    }

}