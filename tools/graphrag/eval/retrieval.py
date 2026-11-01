"""Content-based retrieval diagnostics for the imported multi-hop QA corpus.

The metrics deliberately expose what the wire response proves.  Sentence hits
are conservative (a sentence must occur in one returned chunk); provenance and
trace fields are counted separately so a non-empty response cannot masquerade
as a fully auditable retrieval result.
"""
import json
import math
import unicodedata
import zipfile
from pathlib import Path
from tools.graphrag.eval.metrics import evidence_recall_at_k, reciprocal_rank


def load_query_vectors(path, queries):
    import numpy as np
    query_ids = [str(query['id']) for query in queries]
    if len(query_ids) != len(set(query_ids)):
        raise ValueError('query ids must be unique')
    with np.load(path, allow_pickle=False) as archive:
        ids = [str(value) for value in archive['ids']]
        vectors = archive['vectors']
    if len(ids) != len(set(ids)) or vectors.ndim != 2 or len(ids) != len(vectors):
        raise ValueError('query vectors must have unique ids and a two-dimensional vector matrix')
    if not np.isfinite(vectors).all():
        raise ValueError('query vectors contain non-finite values')
    lookup = {value: index for index, value in enumerate(ids)}
    try:
        return vectors[[lookup[str(query['id'])] for query in queries]]
    except KeyError as error:
        raise ValueError('missing query vector: %s' % error.args[0]) from error


def load_gold(path):
    """Load generic JSONL gold: id, supportingSentences, unresolvedAnnotations."""
    result = {}
    for record in _load_json_records(path):
        query_id = str(record.get('id') or '')
        if not query_id or query_id in result:
            raise ValueError('gold ids must be unique and non-empty')
        sentences = record.get('supportingSentences', [])
        if not isinstance(sentences, list) or any(not isinstance(value, str) for value in sentences):
            raise ValueError('supportingSentences must be a string array')
        result[query_id] = record
    return result


def load_supporting_sentences(path):
    try:
        return {query_id: record.get('supportingSentences', [])
                for query_id, record in load_gold(path).items()}
    except (ValueError, UnicodeDecodeError):
        result = {}
        for record in _load_json_records(path):
            contexts = {title: sentences for title, sentences in record.get('context', [])}
            facts = record.get('supporting_facts', [])
            result[str(record.get('_id') or record.get('id'))] = [
                contexts[title][index] for title, index in facts
                if title in contexts and isinstance(index, int) and 0 <= index < len(contexts[title])]
        return result


def load_supporting_facts(path):
    """Return generic per-query gold records for compatibility."""
    try:
        return load_gold(path)
    except (ValueError, UnicodeDecodeError):
        result = {}
        for record in _load_json_records(path):
            contexts = {title: sentences for title, sentences in record.get('context', [])}
            result[str(record.get('_id') or record.get('id'))] = [
                (title, index, contexts[title][index]) for title, index in record.get('supporting_facts', [])
                if title in contexts and isinstance(index, int) and 0 <= index < len(contexts[title])]
        return result


def load_supporting_targets(gold_path, chunks_path=None):
    """Load sentence, chunk, and relation targets in one deterministic gold pass."""
    chunks_by_document = {}
    if chunks_path is not None:
        with Path(chunks_path).open(encoding='utf-8') as chunks_file:
            for line in chunks_file:
                if not line.strip():
                    continue
                chunk = json.loads(line)
                chunks_by_document.setdefault(str(chunk['document_id']), []).append(chunk)

    result = {}
    for record in _load_json_records(gold_path):
        query_id = str(record.get('document_id') or record.get('_id') or record.get('id') or '')
        if not query_id:
            continue
        sentences = []
        if isinstance(record.get('supportingSentences'), list):
            sentences = [value for value in record['supportingSentences'] if isinstance(value, str)]
        else:
            contexts = {title: values for title, values in record.get('context', [])
                        if isinstance(title, str) and isinstance(values, list)}
            for fact in record.get('supporting_facts', []):
                if not isinstance(fact, (list, tuple)) or len(fact) < 2:
                    continue
                title, index = fact[0], fact[1]
                values = contexts.get(title, [])
                if isinstance(index, int) and 0 <= index < len(values):
                    sentences.append(str(values[index]))
        relations = [str(item[1]) for item in record.get('evidences', [])
                     if isinstance(item, (list, tuple)) and len(item) >= 3]
        candidates = chunks_by_document.get(query_id, [])
        relevant = set()
        unresolved = 0 if chunks_path is not None else None
        for sentence in sentences:
            normalized = normalized_content(sentence)
            matches = [chunk['chunk_id'] for chunk in candidates
                       if normalized and normalized in normalized_content(chunk.get('text', ''))]
            if matches:
                relevant.update(matches)
            elif unresolved is not None:
                unresolved += 1
        result[query_id] = {'supportingSentences': sentences,
                            'relevantChunkIds': sorted(relevant),
                            'unresolvedSupportingFacts': unresolved,
                            'relations': relations}
    return result


def _load_json_records(path):
    if str(path).lower().endswith('.zip'):
        with zipfile.ZipFile(path) as archive:
            names = sorted(name for name in archive.namelist()
                           if name.endswith('.json')
                           and not any(part.startswith('__MACOSX') or part.startswith('._')
                                       for part in Path(name).parts))
            if not names:
                raise ValueError('zip source has no JSON record member')
            selected = next((name for name in names
                             if Path(name).name.lower() == 'dev.json'), names[0])
            value = json.loads(archive.read(selected))
            if not isinstance(value, list):
                raise ValueError('JSON source must contain an array')
            return value
    text = Path(path).read_text(encoding='utf-8')
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        value = [json.loads(line) for line in text.splitlines() if line.strip()]
    if not isinstance(value, list):
        raise ValueError('HotpotQA source must be a JSON array or JSONL file')
    return value


def materialize_queries(source_path, output_path):
    """Write stable generic query records from canonical input records."""
    records = _load_json_records(source_path)
    seen = set()
    with Path(output_path).open('w', encoding='utf-8') as output:
        for record in records:
            query_id = str(record.get('document_id') or record.get('_id') or record.get('id') or '')
            if not query_id or query_id in seen:
                raise ValueError('HotpotQA query ids must be non-empty and unique')
            if not isinstance(record.get('question'), str) or not record['question'].strip():
                raise ValueError('query %s has an empty question' % query_id)
            seen.add(query_id)
            annotations = record.get('annotations', {}) or {}
            if record.get('supporting_facts') is not None:
                annotations = dict(annotations, supporting_facts=record['supporting_facts'])
            output.write(json.dumps({
                'id': query_id,
                'question': record['question'],
                'answer': record.get('answer'),
                'annotations': annotations,
            }, ensure_ascii=False) + '\n')
    return len(seen)


def materialize_hotpot_queries(source_path, output_path):
    """Backward-compatible alias for local adapters; generic output is preferred."""
    return materialize_queries(source_path, output_path)


def supporting_fact_audit(path):
    """Audit generic gold unresolved annotations without rewriting them."""
    try:
        records = list(load_gold(path).values())
    except (ValueError, KeyError):
        records = _load_json_records(path)
    unresolved = []
    if records and 'supportingSentences' in records[0]:
        for record in records:
            unresolved.extend(record.get('unresolvedAnnotations', []))
        total = sum(len(record.get('supportingSentences', []))
                    for record in records) + len(unresolved)
        return {'queries': len(records), 'supportingFacts': total,
                'resolved': total - len(unresolved), 'unresolved': unresolved}
    records = _load_json_records(path)
    unresolved = []
    total = 0
    resolved = 0
    for record in records:
        contexts = {title: sentences for title, sentences in record['context']}
        for title, index in dict.fromkeys(tuple(fact) for fact in record['supporting_facts']):
            total += 1
            if title in contexts and isinstance(index, int) and 0 <= index < len(contexts[title]):
                resolved += 1
            else:
                unresolved.append({'queryId': str(record.get('_id') or record.get('id')),
                                   'title': title, 'sentenceIndex': index,
                                   'availableSentences': len(contexts.get(title, []))})
    return {'queries': len(records), 'supportingFacts': total, 'resolved': resolved,
            'unresolved': unresolved}


def normalized_content(text):
    return ' '.join(unicodedata.normalize('NFC', text).split())


def score_evidence(response, supporting_sentences=None, relevant_chunk_ids=None,
                   relation_targets=None, recall_k=10):
    if not isinstance(response, dict):
        response = {}
    evidence = response.get('evidence') or []
    all_chunks = []
    for item in evidence:
        if not isinstance(item, dict):
            continue
        nested = item.get('chunks') or [item]
        all_chunks.extend(chunk for chunk in nested if isinstance(chunk, dict))
    # Keep duplicate ids from inflating recall, while retaining text from legacy
    # responses which repeated an id with different provenance.
    chunks = {}
    ranked_chunk_ids = []
    for index, chunk in enumerate(all_chunks):
        key = chunk.get('chunkId') or 'response-chunk-%d' % index
        chunks.setdefault(key, []).append(chunk)
        if key not in ranked_chunk_ids:
            ranked_chunk_ids.append(key)
    texts = [normalized_content(chunk.get('text', ''))
             for values in chunks.values() for chunk in values if chunk.get('text')]
    nested_paths = sum(len(item.get('paths') or []) for item in evidence if isinstance(item, dict))
    returned_relation_paths = []
    for item in evidence:
        if not isinstance(item, dict):
            continue
        for trace in item.get('graphTraces') or []:
            labels = [edge.get('label') for edge in trace.get('pathEdges') or []
                      if isinstance(edge, dict) and isinstance(edge.get('label'), str)]
            if labels:
                returned_relation_paths.append([normalized_content(label).lower() for label in labels])
    top_level_paths = response.get('paths') or []
    graph_traces = sum(len(item.get('graphTraces') or []) for item in evidence if isinstance(item, dict))
    source_refs = sum(len(item.get('sources') or []) for item in evidence if isinstance(item, dict))
    source_refs += sum(len(chunk.get('sources') or []) for chunk in all_chunks)
    source_refs += len(response.get('sources') or [])
    trace = response.get('trace')
    trace_ok = int(isinstance(trace, dict) and bool(trace.get('traceVersion'))
                  and bool(trace.get('selectedMode')))
    result = {'nonemptyEvidence': int(bool(texts)),
              'pathCount': nested_paths + len(top_level_paths),
              'nestedPathCount': nested_paths,
              'topLevelPathCount': len(top_level_paths),
              'graphTraceCount': graph_traces,
              'provenanceRefCount': source_refs,
              'tracePresent': int(isinstance(trace, dict)),
              'traceValid': trace_ok,
              'evidenceCount': len(evidence),
              'chunkCount': len(all_chunks),
              'evidenceRecallAtK': None, 'mrr': None, 'ndcgAtK': None,
              'relationPathHit': None,
              'supportingSentenceCount': None, 'supportingSentencesHit': None,
              'supportingSentenceRecall': None, 'allSupportingSentencesHit': None}
    if relevant_chunk_ids is not None:
        relevant = set(relevant_chunk_ids)
        recall = evidence_recall_at_k(ranked_chunk_ids, relevant, recall_k)
        result.update(evidenceRecallAtK=recall,
                      mrr=reciprocal_rank(ranked_chunk_ids, relevant),
                      ndcgAtK=_ndcg_at_k(ranked_chunk_ids, relevant, recall_k))
    if relation_targets is not None:
        target = [normalized_content(value).lower() for value in relation_targets]
        result['relationPathHit'] = int(bool(target) and any(
            _contains_sequence(path, target) for path in returned_relation_paths))
    if supporting_sentences:
        hit = sum(any(normalized_content(sentence) in text for text in texts)
                  for sentence in supporting_sentences)
        result.update(supportingSentenceCount=len(supporting_sentences), supportingSentencesHit=hit,
                      supportingSentenceRecall=hit / len(supporting_sentences),
                      allSupportingSentencesHit=int(hit == len(supporting_sentences)))
    return result


def _ndcg_at_k(ranked, relevant, k):
    if not relevant or k < 1:
        return 0.0
    dcg = sum(1.0 / math.log2(index + 2)
              for index, value in enumerate(ranked[:k]) if value in relevant)
    ideal = sum(1.0 / math.log2(index + 2)
                for index in range(min(k, len(relevant))))
    return dcg / ideal if ideal else 0.0


def _contains_sequence(values, target):
    return any(values[index:index + len(target)] == target
               for index in range(len(values) - len(target) + 1))
