"""Answer and evidence metrics used by the reproducible benchmark."""
import string
from collections import Counter
from typing import Iterable, Sequence


def normalize_answer(value: str) -> str:
    value = value.lower()
    value = ''.join(ch for ch in value if ch not in string.punctuation)
    return ' '.join(value.split())


def exact_match(prediction: str, answer: str) -> float:
    return float(normalize_answer(prediction) == normalize_answer(answer))


def f1(prediction: str, answer: str) -> float:
    predicted = normalize_answer(prediction).split()
    expected = normalize_answer(answer).split()
    if not predicted or not expected:
        return float(predicted == expected)
    overlap = sum((Counter(predicted) & Counter(expected)).values())
    if not overlap:
        return 0.0
    precision = overlap / len(predicted)
    recall = overlap / len(expected)
    return 2 * precision * recall / (precision + recall)


def evidence_recall_at_k(ranked: Sequence[str], relevant: Iterable[str], k: int) -> float:
    target = set(relevant)
    return len(target.intersection(ranked[:k])) / len(target) if target else 0.0


def reciprocal_rank(ranked: Sequence[str], relevant: Iterable[str]) -> float:
    target = set(relevant)
    for index, item in enumerate(ranked, 1):
        if item in target:
            return 1.0 / index
    return 0.0


def percentile(values: Sequence[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)
