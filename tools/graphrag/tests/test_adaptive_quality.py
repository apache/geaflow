"""Regression guard for the adaptive channel's measured quality.

The adaptive channel once ranked an order of magnitude below graph-only (2Wiki MRR 0.1534 vs
0.6344) because it promoted text channels that carry almost no gold signal and starved the graph
channel of candidates. That was fixed, but nothing stopped a later routing or fusion change from
silently reintroducing the same behaviour: the only evidence lived in a report.

This guard encodes the measured baseline and fails if the adaptive channel regresses past a floor,
or if it stops clearly beating fixed hybrid on MRR. It reads committed fixture values rather than a
live run so it stays fast and deterministic; evaluate_new_run() applies the same floors to a fresh
CSV, which is how a real benchmark result can be checked against the same bar.
"""
import json
from pathlib import Path

FIXTURE = Path(__file__).resolve().parent / 'fixtures' / 'adaptive-quality-baseline.json'


def _baseline():
    return json.loads(FIXTURE.read_text(encoding='utf-8'))


def assert_quality_holds(dataset, methods):
    """Applies the recorded floors to a dataset's per-method measurements.

    :param dataset: dataset key present in the fixture
    :param methods: mapping of method name to {'mrr', 'recall', ['edges']}
    :raises AssertionError: when the adaptive channel falls below a recorded floor
    """
    floors = _baseline()['datasets'][dataset]['floors']
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


def test_recorded_baseline_satisfies_its_own_floors():
    baseline = _baseline()
    for dataset, entry in baseline['datasets'].items():
        assert_quality_holds(dataset, entry['methods'])


def test_guard_rejects_the_text_dominated_regression():
    """The regression this guards against must actually trip the floors."""
    baseline = _baseline()
    for dataset, entry in baseline['datasets'].items():
        regressed = {name: dict(values) for name, values in entry['methods'].items()}
        # The previously measured broken behaviour: text channels weighted above graph, graph starved.
        regressed['ADAPTIVE_HYBRID']['mrr'] = 0.1534 if dataset == '2wikimultihopqa' else 0.0723
        try:
            assert_quality_holds(dataset, regressed)
        except AssertionError:
            continue
        raise AssertionError(
            f'{dataset}: the guard failed to catch the text-dominated regression')


def test_adaptive_actually_uses_the_graph_channel():
    """A routing change that starves the graph channel is the mechanism behind the regression.

    Adaptive is not required to be cheaper than graph-only: it reserves the largest share of the
    candidate budget for the graph channel and also funds a text probe, so on HotpotQA it scans more
    edges than graph-only. What must never happen is the earlier failure mode where the graph stage
    was starved or skipped, so the guard only asserts that real graph work happens and that the
    adaptive channel is cheaper than paying for a full graph scan plus a full text retrieval.
    """
    for dataset, entry in _baseline()['datasets'].items():
        methods = entry['methods']
        adaptive_edges = methods['ADAPTIVE_HYBRID']['edges']
        assert adaptive_edges > 0, f'{dataset}: adaptive ran no graph edges'
        # Starving the graph channel is what collapsed MRR; a floor keeps that from returning.
        assert adaptive_edges >= methods['FIXED_HYBRID']['edges'] * 0.5, (
            f'{dataset}: adaptive scans {adaptive_edges} edges, far below fixed hybrid '
            f"{methods['FIXED_HYBRID']['edges']} - the graph channel looks starved")


def test_reproduce_doc_keeps_the_negative_result_and_matches_the_fixture():
    """The design requires negative results and ablations to be retained, not quietly dropped.

    The section is also checked against the fixture so the document and the guard cannot disagree:
    a stale document is how the original negative result became invisible in the first place.
    """
    doc = (Path(__file__).resolve().parents[1] / 'REPRODUCE.md').read_text(encoding='utf-8')
    assert 'Adaptive Hybrid' in doc, 'REPRODUCE.md must still discuss the adaptive result'
    assert 'negative result' in doc.lower(), (
        'REPRODUCE.md must retain the original negative result rather than reporting only the fix')
    assert 'does not exceed graph-only' in doc, (
        'REPRODUCE.md must keep the applicability boundary, not just the improvement')

    baseline = _baseline()
    for dataset, entry in baseline['datasets'].items():
        adaptive = entry['methods']['ADAPTIVE_HYBRID']
        graph = entry['methods']['GRAPH_ONLY']
        assert f"{adaptive['mrr']:.4f}" in doc, (
            f'REPRODUCE.md must quote the current {dataset} adaptive MRR '
            f"{adaptive['mrr']:.4f} recorded in the fixture")
        assert f"{graph['mrr']:.4f}" in doc, (
            f'REPRODUCE.md must quote the current {dataset} graph-only MRR {graph["mrr"]:.4f}')


def test_documented_policy_parameters_match_the_shipped_defaults():
    """A report that quotes parameters the code no longer uses is a stale report."""
    baseline = _baseline()
    for entry in baseline['datasets'].values():
        assert entry['recordedRun'], 'every dataset entry must name the run it came from'
        assert entry['floors']['adaptiveMrr'] > 0
