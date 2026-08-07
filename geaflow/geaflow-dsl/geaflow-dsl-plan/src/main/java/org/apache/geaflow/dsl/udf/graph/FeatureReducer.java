/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.dsl.udf.graph;

import java.util.List;

/**
 * Feature reducer for selecting important feature dimensions to reduce transmission overhead.
 *
 * <p>This class implements feature selection by keeping only the most important dimensions
 * from the full feature vector. This significantly reduces the amount of data transferred
 * between Java and Python processes, improving performance for large feature vectors.
 *
 * <p>Usage:
 * <pre>
 *   // Select first 64 dimensions
 *   int[] selectedDims = new int[64];
 *   for (int i = 0; i < 64; i++) {
 *       selectedDims[i] = i;
 *   }
 *   FeatureReducer reducer = new FeatureReducer(selectedDims);
 *   double[] reduced = reducer.reduceFeatures(fullFeatures);
 * </pre>
 *
 * <p>Benefits:
 * - Reduces memory usage for feature storage
 * - Reduces network/IO overhead in Java-Python communication
 * - Improves inference speed by processing smaller feature vectors
 * - Maintains model accuracy if important dimensions are selected correctly
 */
public class FeatureReducer {

    private final int[] selectedDimensions;

    /**
     * Creates a feature reducer with specified dimension indices.
     *
     * @param selectedDimensions Array of dimension indices to keep.
     *                          Indices should be valid for the full feature vector.
     *                          Duplicate indices are allowed but not recommended.
     */
    public FeatureReducer(int[] selectedDimensions) {
        if (selectedDimensions == null || selectedDimensions.length == 0) {
            throw new IllegalArgumentException(
                "Selected dimensions array cannot be null or empty");
        }
        this.selectedDimensions = selectedDimensions.clone(); // Defensive copy
    }

    /**
     * Reduces a full feature vector to selected dimensions.
     *
     * @param fullFeatures The complete feature vector
     * @return Reduced feature vector containing only selected dimensions
     * @throws IllegalArgumentException if fullFeatures is null or too short
     */
    public double[] reduceFeatures(double[] fullFeatures) {
        if (fullFeatures == null) {
            throw new IllegalArgumentException("Full features array cannot be null");
        }
        
        double[] reducedFeatures = new double[selectedDimensions.length];
        int maxDim = getMaxDimension();
        
        if (maxDim >= fullFeatures.length) {
            throw new IllegalArgumentException(
                String.format("Feature vector length (%d) is too short for selected dimensions (max: %d)",
                    fullFeatures.length, maxDim + 1));
        }
        
        for (int i = 0; i < selectedDimensions.length; i++) {
            int dimIndex = selectedDimensions[i];
            reducedFeatures[i] = fullFeatures[dimIndex];
        }
        
        return reducedFeatures;
    }

    /**
     * Reduces a feature list to selected dimensions.
     *
     * @param fullFeatures The complete feature list
     * @return Reduced feature array containing only selected dimensions
     */
    public double[] reduceFeatures(List<Double> fullFeatures) {
        if (fullFeatures == null) {
            throw new IllegalArgumentException("Full features list cannot be null");
        }
        
        double[] fullArray = new double[fullFeatures.size()];
        for (int i = 0; i < fullFeatures.size(); i++) {
            Double value = fullFeatures.get(i);
            fullArray[i] = value != null ? value : 0.0;
        }
        
        return reduceFeatures(fullArray);
    }

    /**
     * Reduces multiple feature vectors in batch.
     *
     * @param fullFeaturesList List of full feature vectors
     * @return Array of reduced feature vectors
     */
    public double[][] reduceFeaturesBatch(List<double[]> fullFeaturesList) {
        if (fullFeaturesList == null) {
            throw new IllegalArgumentException("Full features list cannot be null");
        }
        
        double[][] reducedFeatures = new double[fullFeaturesList.size()][];
        for (int i = 0; i < fullFeaturesList.size(); i++) {
            reducedFeatures[i] = reduceFeatures(fullFeaturesList.get(i));
        }
        
        return reducedFeatures;
    }

    /**
     * Gets the maximum dimension index in the selected dimensions.
     *
     * @return Maximum dimension index
     */
    private int getMaxDimension() {
        int max = selectedDimensions[0];
        for (int dim : selectedDimensions) {
            if (dim > max) {
                max = dim;
            }
        }
        return max;
    }

    /**
     * Gets the number of selected dimensions.
     *
     * @return Number of dimensions in the reduced feature vector
     */
    public int getReducedDimension() {
        return selectedDimensions.length;
    }

    /**
     * Gets the selected dimension indices.
     *
     * @return Copy of the selected dimension indices array
     */
    public int[] getSelectedDimensions() {
        return selectedDimensions.clone(); // Defensive copy
    }

    /**
     * Creates a feature reducer that selects the first N dimensions.
     *
     * <p>This is a convenience method for the common case of selecting
     * the first N dimensions from a feature vector.
     *
     * @param numDimensions Number of dimensions to select from the beginning
     * @return FeatureReducer instance
     */
    public static FeatureReducer selectFirst(int numDimensions) {
        if (numDimensions <= 0) {
            throw new IllegalArgumentException(
                "Number of dimensions must be positive, got: " + numDimensions);
        }
        
        int[] dims = new int[numDimensions];
        for (int i = 0; i < numDimensions; i++) {
            dims[i] = i;
        }
        
        return new FeatureReducer(dims);
    }

    /**
     * Creates a feature reducer that selects evenly spaced dimensions.
     *
     * <p>This method selects dimensions at regular intervals, which can be useful
     * for uniform sampling across the feature space.
     *
     * @param numDimensions Number of dimensions to select
     * @param totalDimensions Total number of dimensions in the full feature vector
     * @return FeatureReducer instance
     */
    public static FeatureReducer selectEvenlySpaced(int numDimensions, int totalDimensions) {
        if (numDimensions <= 0) {
            throw new IllegalArgumentException(
                "Number of dimensions must be positive, got: " + numDimensions);
        }
        if (totalDimensions <= 0) {
            throw new IllegalArgumentException(
                "Total dimensions must be positive, got: " + totalDimensions);
        }
        if (numDimensions > totalDimensions) {
            throw new IllegalArgumentException(
                String.format("Cannot select %d dimensions from %d total dimensions",
                    numDimensions, totalDimensions));
        }
        
        int[] dims = new int[numDimensions];
        double step = (double) totalDimensions / numDimensions;
        for (int i = 0; i < numDimensions; i++) {
            dims[i] = (int) Math.floor(i * step);
        }
        
        return new FeatureReducer(dims);
    }
}

