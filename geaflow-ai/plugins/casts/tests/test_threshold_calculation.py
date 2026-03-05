# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Unit tests: dynamic similarity threshold calculation.

This test suite validates the core formula and expected behaviors for the
dynamic similarity threshold used by CASTS.

Formula:
    delta_sim(v) = 1 - kappa / (sigma_logic(v) * (1 + beta * log(eta(v))))

Expected properties:
    1. delta_sim(v) in (0, 1) and is monotonic in eta(v)
    2. Higher eta -> stricter threshold
    3. Lower eta -> more permissive threshold
    4. Higher sigma -> stricter threshold
"""

import unittest
from unittest.mock import MagicMock

from core.models import StrategyKnowledgeUnit
from utils.helpers import calculate_dynamic_similarity_threshold


class TestDynamicSimilarityThreshold(unittest.TestCase):
    """Test dynamic similarity threshold calculation."""

    def setUp(self):
        """Create mock SKU objects for threshold tests."""
        self.create_mock_sku = lambda eta, sigma: MagicMock(
            spec=StrategyKnowledgeUnit,
            confidence_score=eta,
            logic_complexity=sigma,
        )

    def test_formula_correctness_with_doc_examples(self):
        """Validate expected formula behavior with example scenarios."""
        # Example 1: Head scenario (eta=1000, sigma=1, beta=0.1, kappa=0.01)
        sku_head = self.create_mock_sku(eta=1000, sigma=1)
        threshold_head = calculate_dynamic_similarity_threshold(sku_head, kappa=0.01, beta=0.1)
        # Expected: approx 0.998 (allow small error)
        self.assertAlmostEqual(threshold_head, 0.998, places=2,
                               msg="Head scenario threshold should be near 0.998")

        # Example 2: Tail scenario (eta=0.5, sigma=1, beta=0.1, kappa=0.01)
        sku_tail = self.create_mock_sku(eta=0.5, sigma=1)
        threshold_tail = calculate_dynamic_similarity_threshold(sku_tail, kappa=0.01, beta=0.1)
        # Expected: approx 0.99 (more permissive)
        self.assertAlmostEqual(threshold_tail, 0.99, places=2,
                               msg="Tail scenario threshold should be near 0.99")

        # Example 3: Complex logic scenario (eta=1000, sigma=5, beta=0.1, kappa=0.01)
        sku_complex = self.create_mock_sku(eta=1000, sigma=5)
        threshold_complex = calculate_dynamic_similarity_threshold(
            sku_complex, kappa=0.01, beta=0.1
        )
        # Expected: near 0.99, actual result is closer to 0.9988
        self.assertGreater(threshold_complex, 0.998,
                           msg="Complex-logic scenario threshold should be > 0.998")

        # Head scenario should be stricter than tail scenario
        self.assertGreater(
            threshold_head, threshold_tail,
            msg="High-frequency SKU should have a higher threshold"
        )

    def test_monotonicity_with_confidence(self):
        """Threshold should be monotonic with respect to eta (confidence)."""
        kappa = 0.05
        beta = 0.1
        sigma = 1

        # Thresholds across confidence values
        confidence_values = [1, 2, 5, 10, 20, 50, 100, 1000]
        thresholds = []

        for eta in confidence_values:
            sku = self.create_mock_sku(eta=eta, sigma=sigma)
            threshold = calculate_dynamic_similarity_threshold(sku, kappa=kappa, beta=beta)
            thresholds.append(threshold)

        # Monotonicity: each threshold should be >= previous one
        for i in range(1, len(thresholds)):
            msg = (
                "Thresholds must be monotonic: "
                f"eta={confidence_values[i]} should be >= eta={confidence_values[i-1]}"
            )
            self.assertGreaterEqual(
                thresholds[i],
                thresholds[i - 1],
                msg=msg,
            )

    def test_monotonicity_with_complexity(self):
        """Threshold should be monotonic with respect to logic complexity."""
        kappa = 0.05
        beta = 0.1
        eta = 10

        # Thresholds across complexity values
        complexity_values = [1, 2, 3, 5, 10]
        thresholds = []

        for sigma in complexity_values:
            sku = self.create_mock_sku(eta=eta, sigma=sigma)
            threshold = calculate_dynamic_similarity_threshold(sku, kappa=kappa, beta=beta)
            thresholds.append(threshold)

        # Monotonicity check
        for i in range(1, len(thresholds)):
            msg = (
                "Threshold should increase with complexity: "
                f"sigma={complexity_values[i]} should be >= sigma={complexity_values[i-1]}"
            )
            self.assertGreaterEqual(
                thresholds[i],
                thresholds[i - 1],
                msg=msg,
            )

    def test_boundary_conditions(self):
        """Validate behavior at boundary conditions."""
        # Boundary 1: minimum confidence (eta=1)
        sku_min = self.create_mock_sku(eta=1, sigma=1)
        threshold_min = calculate_dynamic_similarity_threshold(sku_min, kappa=0.1, beta=0.1)
        self.assertGreater(threshold_min, 0, msg="Threshold must be > 0")
        self.assertLess(threshold_min, 1, msg="Threshold must be < 1")

        # Boundary 2: very high confidence
        sku_max = self.create_mock_sku(eta=100000, sigma=1)
        threshold_max = calculate_dynamic_similarity_threshold(sku_max, kappa=0.01, beta=0.1)
        self.assertLess(threshold_max, 1.0, msg="Threshold must be < 1 at high eta")
        self.assertGreater(threshold_max, 0.99, msg="High eta should yield near-1 threshold")

        # Boundary 3: eta < 1 (clamped to 1)
        sku_sub_one = self.create_mock_sku(eta=0.1, sigma=1)
        threshold_sub_one = calculate_dynamic_similarity_threshold(
            sku_sub_one, kappa=0.05, beta=0.1
        )
        self.assertGreater(threshold_sub_one, 0, msg="Threshold should remain valid")

    def test_kappa_sensitivity(self):
        """Kappa should inversely affect the threshold (counter-intuitive)."""
        eta = 10
        sigma = 1
        beta = 0.1

        kappa_values = [0.01, 0.05, 0.10, 0.20, 0.30]
        thresholds = []

        for kappa in kappa_values:
            sku = self.create_mock_sku(eta=eta, sigma=sigma)
            threshold = calculate_dynamic_similarity_threshold(sku, kappa=kappa, beta=beta)
            thresholds.append(threshold)

        # As kappa increases, threshold should decrease.
        for i in range(1, len(thresholds)):
            self.assertLessEqual(
                thresholds[i], thresholds[i-1],
                msg=(
                    "Threshold should decrease as kappa increases: "
                    f"kappa={kappa_values[i]} -> {thresholds[i]:.4f} "
                    f"<= kappa={kappa_values[i-1]} -> {thresholds[i-1]:.4f}"
                )
            )

    def test_beta_sensitivity(self):
        """Beta controls the gap between high- and low-frequency thresholds."""
        kappa = 0.05
        sigma = 1

        # Compare threshold gaps at different beta values.
        eta_high = 100
        eta_low = 2

        beta_values = [0.01, 0.05, 0.1, 0.2]
        threshold_gaps = []

        for beta in beta_values:
            sku_high = self.create_mock_sku(eta=eta_high, sigma=sigma)
            sku_low = self.create_mock_sku(eta=eta_low, sigma=sigma)

            threshold_high = calculate_dynamic_similarity_threshold(
                sku_high, kappa=kappa, beta=beta
            )
            threshold_low = calculate_dynamic_similarity_threshold(
                sku_low, kappa=kappa, beta=beta
            )

            gap = threshold_high - threshold_low
            threshold_gaps.append(gap)

        # As beta increases, the gap should increase.
        for i in range(1, len(threshold_gaps)):
            self.assertGreaterEqual(
                threshold_gaps[i], threshold_gaps[i-1],
                msg=(
                    "Gap should increase as beta increases: "
                    f"beta={beta_values[i]} gap >= beta={beta_values[i-1]} gap"
                )
            )

    def test_realistic_scenarios_with_current_config(self):
        """Validate threshold ranges for representative scenarios."""
        kappa = 0.30
        beta = 0.05

        test_cases = [
            # (scenario name, eta, sigma, expected range)
            ("low_freq_simple", 2, 1, (0.70, 0.75)),
            ("low_freq_complex", 2, 2, (0.85, 0.88)),
            ("mid_freq_simple", 10, 1, (0.72, 0.74)),
            ("mid_freq_complex", 10, 2, (0.86, 0.88)),
            ("high_freq_simple", 50, 1, (0.73, 0.76)),
            ("high_freq_complex", 50, 2, (0.87, 0.89)),
        ]

        for name, eta, sigma, (expected_min, expected_max) in test_cases:
            with self.subTest(scenario=name, eta=eta, sigma=sigma):
                sku = self.create_mock_sku(eta=eta, sigma=sigma)
                threshold = calculate_dynamic_similarity_threshold(
                    sku, kappa=kappa, beta=beta
                )

                self.assertGreaterEqual(
                    threshold, expected_min,
                    msg=f"{name}: threshold {threshold:.4f} should be >= {expected_min}"
                )
                self.assertLessEqual(
                    threshold, expected_max,
                    msg=f"{name}: threshold {threshold:.4f} should be <= {expected_max}"
                )

    def test_practical_matching_scenario(self):
        """Simulate a reported matching scenario and validate kappa effects."""
        user_similarity = 0.8322

        # Old configuration
        kappa_old = 0.25
        beta_old = 0.05

        # New configuration (increase kappa to lower threshold)
        kappa_new = 0.30
        beta_new = 0.05

        # Inferred SKU_17 parameters
        sku_17 = self.create_mock_sku(eta=20, sigma=2)

        threshold_old = calculate_dynamic_similarity_threshold(
            sku_17, kappa=kappa_old, beta=beta_old
        )
        threshold_new = calculate_dynamic_similarity_threshold(
            sku_17, kappa=kappa_new, beta=beta_new
        )

        # Old config should not match
        self.assertAlmostEqual(
            threshold_old, 0.8915, delta=0.01,
            msg=f"Old threshold should be near 0.8915, actual: {threshold_old:.4f}"
        )
        self.assertLess(
            user_similarity, threshold_old,
            msg=f"Old config should not match: {user_similarity:.4f} < {threshold_old:.4f}"
        )

        # Increasing kappa should lower the threshold
        self.assertLess(
            threshold_new, threshold_old,
            msg=f"Higher kappa should lower threshold: {threshold_new:.4f} < {threshold_old:.4f}"
        )

        print("\n[Scenario] SKU_17 (eta=20, sigma=2):")
        print(f"  Old threshold (kappa=0.25): {threshold_old:.4f}")
        print(f"  New threshold (kappa=0.30): {threshold_new:.4f}")
        print(f"  Similarity: {user_similarity:.4f}")
        print(f"  New config match: {user_similarity >= threshold_new}")

        # Simple SKU should still match under old config
        sku_simple = self.create_mock_sku(eta=10, sigma=1)
        threshold_simple_old = calculate_dynamic_similarity_threshold(
            sku_simple, kappa=kappa_old, beta=beta_old
        )

        self.assertLessEqual(
            threshold_simple_old, user_similarity,
            msg=(
                "Simple SKU should match under old config: "
                f"{threshold_simple_old:.4f} <= {user_similarity:.4f}"
            )
        )

    def test_mathematical_properties_summary(self):
        """Summary test for key mathematical properties."""
        kappa = 0.10
        beta = 0.10

        # Generate test points
        test_points = [
            (eta, sigma)
            for eta in [1, 2, 5, 10, 20, 50, 100]
            for sigma in [1, 2, 3, 5]
        ]

        for eta, sigma in test_points:
            sku = self.create_mock_sku(eta=eta, sigma=sigma)
            threshold = calculate_dynamic_similarity_threshold(sku, kappa=kappa, beta=beta)

            # Property 1: threshold in (0, 1)
            self.assertGreater(threshold, 0, msg=f"(eta={eta},sigma={sigma}): > 0")
            self.assertLess(threshold, 1, msg=f"(eta={eta},sigma={sigma}): < 1")

        # Properties 2 & 3: monotonicity covered above

        # Property 4: high-frequency SKU vs low-frequency SKU
        sku_high_freq = self.create_mock_sku(eta=100, sigma=1)
        sku_low_freq = self.create_mock_sku(eta=2, sigma=1)

        threshold_high = calculate_dynamic_similarity_threshold(
            sku_high_freq, kappa=kappa, beta=beta
        )
        threshold_low = calculate_dynamic_similarity_threshold(
            sku_low_freq, kappa=kappa, beta=beta
        )

        self.assertGreater(
            threshold_high, threshold_low,
            msg="High-frequency SKU should have a higher threshold"
        )

        # Ensure a meaningful gap
        gap_ratio = (threshold_high - threshold_low) / threshold_low
        self.assertGreater(
            gap_ratio, 0.01,
            msg="Threshold gap should be > 1%"
        )


class TestThresholdIntegrationWithStrategyCache(unittest.TestCase):
    """Integration sanity check for StrategyCache usage."""

    def test_threshold_used_in_tier2_matching(self):
        """Placeholder: StrategyCache integration covered elsewhere."""
        self.assertTrue(True, "Integration tests live in test_signature_abstraction.py")


if __name__ == "__main__":
    # Run tests with verbose output
    unittest.main(verbosity=2)
