"""Main entry point for CASTS strategy cache simulations."""

import asyncio
from typing import Any

from casts.core.config import DefaultConfiguration
from casts.core.services import StrategyCache
from casts.data.sources import DataSourceFactory
from casts.services.embedding import EmbeddingService
from casts.services.llm_oracle import LLMOracle
from casts.services.path_judge import PathJudge
from casts.simulation.engine import SimulationEngine
from casts.simulation.evaluator import BatchEvaluator, PathEvaluationScore, PathEvaluator
from casts.simulation.metrics import MetricsCollector
from casts.simulation.visualizer import SimulationVisualizer


async def run_simulation():
    """
    Run a CASTS strategy cache simulation.

    All configuration parameters are loaded from DefaultConfiguration.
    """
    # Initialize configuration
    config = DefaultConfiguration()

    # Initialize data source using factory, which now reads from config
    graph = DataSourceFactory.create(config)

    # Initialize services with configuration
    embed_service = EmbeddingService(config)
    strategy_cache = StrategyCache(embed_service, config=config)
    llm_oracle = LLMOracle(embed_service, config)
    path_judge = PathJudge(config)

    # Setup verifier if enabled
    batch_evaluator = None
    schema_summary: dict[str, Any] = {}
    all_evaluation_results: dict[int, PathEvaluationScore] = {}
    if config.get_bool("SIMULATION_ENABLE_VERIFIER"):
        schema_summary = {
            "node_types": list(graph.get_schema().node_types),
            "edge_labels": list(graph.get_schema().edge_labels),
        }
        evaluator = PathEvaluator(llm_judge=path_judge)
        batch_evaluator = BatchEvaluator(evaluator)

    # Create and run simulation engine
    engine = SimulationEngine(
        graph=graph,
        strategy_cache=strategy_cache,
        llm_oracle=llm_oracle,
        max_depth=config.get_int("SIMULATION_MAX_DEPTH"),
        verbose=config.get_bool("SIMULATION_VERBOSE_LOGGING"),
    )

    # Define the callback for completed requests
    def evaluate_completed_request(request_id: int, metrics_collector: MetricsCollector):
        if not batch_evaluator or not config.get_bool("SIMULATION_ENABLE_VERIFIER"):
            return

        print(f"\n[Request {request_id} Verifier]")
        path_data = metrics_collector.paths.get(request_id)
        if not path_data:
            print("  No path data found for this request.")
            return

        # Evaluate a single path
        results, metadata = batch_evaluator.evaluate_batch(
            {request_id: path_data}, schema=schema_summary
        )
        if results:
            all_evaluation_results.update(results)
            batch_evaluator.print_batch_summary(results, metadata)

    # Run simulation
    metrics_collector = await engine.run_simulation(
        num_epochs=config.get_int("SIMULATION_NUM_EPOCHS"),
        on_request_completed=evaluate_completed_request
    )

    # Get sorted SKUs for reporting
    sorted_skus = sorted(
        strategy_cache.knowledge_base,
        key=lambda x: x.confidence_score,
        reverse=True
    )

    # Print results
    # Print final evaluation summary if verifier is enabled
    if config.get_bool("SIMULATION_ENABLE_VERIFIER") and batch_evaluator:
        batch_evaluator.print_batch_summary(all_evaluation_results)

    # Generate and save visualization if enabled
    if config.get_bool("SIMULATION_ENABLE_VISUALIZER"):
        print("\nPrinting final simulation results...")
        await SimulationVisualizer.print_all_results(
            paths=metrics_collector.paths,
            metrics=metrics_collector.metrics,
            cache=strategy_cache,
            sorted_skus=sorted_skus,
            graph=graph,
            show_plots=False,
        )
        print("Simulation visualizations saved to files.")

    return metrics_collector


def main():
    """Convenience entry point for running simulations from Python code.

    All configuration parameters are loaded from DefaultConfiguration.
    This avoids a CLI parser and lets notebooks / scripts call ``main`` directly.
    """

    print("CASTS Strategy Cache Simulation")
    print("=" * 40)

    asyncio.run(run_simulation())

    print("\n" + "=" * 40)
    print("Simulation completed successfully!")


if __name__ == "__main__":
    main()
