package org.workflowsim.predictive;

import java.io.FileInputStream;
import java.io.FileReader;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.workflowsim.Job;
import weka.classifiers.Classifier;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SerializationHelper;

public class WekaPredictor {

    private static Classifier classifier;
    private static Instances dataHeader;

    static {
        try {
            Log.printLine("Initializing WekaPredictor...");
            String modelPath = "predictive_heft_model.model";
            String arffPath = "training_data.arff";

            classifier = (Classifier) SerializationHelper.read(new FileInputStream(modelPath));
            Log.printLine("Successfully loaded Weka model from: " + modelPath);

            FileReader reader = new FileReader(arffPath);
            dataHeader = new Instances(reader);
            reader.close();

            if (dataHeader.classIndex() == -1) {
                dataHeader.setClassIndex(dataHeader.numAttributes() - 1);
            }
            Log.printLine("Successfully loaded ARFF header for creating new instances.");

        } catch (Exception e) {
            Log.printLine("Error initializing WekaPredictor. Please check model/arff file paths.");
            e.printStackTrace();
            classifier = null;
            dataHeader = null;
        }
    }

    public static double predictExecutionTime(Job job, Vm vm) {
        if (classifier == null || dataHeader == null) {
            Log.printLine("WekaPredictor not initialized. Returning default execution time.");
            return job.getCloudletLength() / vm.getMips();
        }
        try {
            Instance instance = new DenseInstance(dataHeader.numAttributes());
            instance.setDataset(dataHeader);
            instance.setValue(dataHeader.attribute("task_length"), job.getCloudletLength());
            instance.setValue(dataHeader.attribute("num_parents"), job.getParentList().size());
            instance.setValue(dataHeader.attribute("vm_mips"), vm.getMips());
            instance.setValue(dataHeader.attribute("vm_pes"), vm.getNumberOfPes());
            return classifier.classifyInstance(instance);
        } catch (Exception e) {
            Log.printLine("Error during Weka prediction: " + e.getMessage());
            e.printStackTrace();
            return job.getCloudletLength() / vm.getMips();
        }
    }
}
