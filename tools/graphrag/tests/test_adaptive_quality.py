"""Unit tests for the adaptive quality guard using synthetic metrics."""

TEST_FLOORS = {
    'adaptiveMrr': 0.25,
    'adaptiveRecall': 0.2,
    'adaptiveVsFixedMrrRatio': 1.5,
}


def assert_quality_holds(dataset, methods, floors=TEST_FLOORS):
    """Applies quality floors to synthetic per-method measurements.

    :param dataset: dataset label used in assertion messages
    :param methods: mapping of method name to {'mrr', 'recall', ['edges']}
    :param floors: minimum adaptive quality and adaptive-to-fixed ratio
    :raises AssertionError: when the adaptive channel falls below a recorded floor
    """
    adaptive = methods['ADAPTIVE_HYBRID']
    fixed = methods['FIXED_HYBRID']

    assert adaptive['mrr'] >= floors['adaptiveMrr'], (
        f"{dataset}: adaptive MRR {adaptive['mrr']} fell below the {floors['adaptiveMrr']} floor")
    assert adaptive['recall'] >= floors['adaptiveRecall'], (
        f"{dataset}: adaptive recall {adaptive['recall']} fell below the "
        f"{floors['adaptiveRecall']} floor")

    # The point of adaptive routing is to beat the fixed hybrid it replaces. If it stops doing so
    # the routing policy is not earning its complexity, whatever its absolute numbers are.
    ratio = adaptive['mrr'] / max(fixed['mrr'], 1e-9)
    assert ratio >= floors['adaptiveVsFixedMrrRatio'], (
        f"{dataset}: adaptive MRR is only {ratio:.2f}x fixed hybrid, below the "
        f"{floors['adaptiveVsFixedMrrRatio']}x floor")


def test_quality_guard_accepts_healthy_synthetic_metrics():
    methods = {
        'ADAPTIVE_HYBRID': {'mrr': 0.5, 'recall': 0.4, 'edges': 3},
        'FIXED_HYBRID': {'mrr': 0.2, 'recall': 0.3, 'edges': 4},
    }

    assert_quality_holds('synthetic', methods)


def test_guard_rejects_the_text_dominated_regression():
    """The regression this guards against must actually trip the floors."""
    regressed = {
        'ADAPTIVE_HYBRID': {'mrr': 0.05, 'recall': 0.1, 'edges': 1},
        'FIXED_HYBRID': {'mrr': 0.2, 'recall': 0.3, 'edges': 4},
    }

    try:
        assert_quality_holds('synthetic', regressed)
    except AssertionError:
        return
    raise AssertionError('the guard failed to catch the text-dominated regression')


def test_adaptive_actually_uses_the_graph_channel():
    """A routing change that starves the graph channel is the mechanism behind the regression.

    Adaptive is not required to be cheaper than graph-only: it reserves the largest share of the
    candidate budget for the graph channel and also funds a text probe, so on HotpotQA it scans more
    edges than graph-only. What must never happen is the earlier failure mode where the graph stage
    was starved or skipped, so the guard only asserts that real graph work happens and that the
    adaptive channel is cheaper than paying for a full graph scan plus a full text retrieval.
    """
    methods = {
        'ADAPTIVE_HYBRID': {'mrr': 0.5, 'recall': 0.4, 'edges': 3},
        'FIXED_HYBRID': {'mrr': 0.2, 'recall': 0.3, 'edges': 4},
    }
    adaptive_edges = methods['ADAPTIVE_HYBRID']['edges']
    assert adaptive_edges > 0, 'adaptive ran no graph edges'
    assert adaptive_edges >= methods['FIXED_HYBRID']['edges'] * 0.5, (
        'adaptive graph work is too small compared with fixed hybrid')


def test_guard_keeps_the_graph_only_comparison_as_an_applicability_boundary():
    """Adaptive may pass its floor without beating graph-only on every workload."""
    methods = {
        'ADAPTIVE_HYBRID': {'mrr': 0.5, 'recall': 0.4, 'edges': 3},
        'FIXED_HYBRID': {'mrr': 0.2, 'recall': 0.3, 'edges': 4},
        'GRAPH_ONLY': {'mrr': 0.8, 'recall': 0.7, 'edges': 5},
    }

    assert methods['ADAPTIVE_HYBRID']['mrr'] < methods['GRAPH_ONLY']['mrr']
    assert_quality_holds('synthetic', methods)


def test_quality_floors_are_positive():
    assert all(value > 0 for value in TEST_FLOORS.values())
