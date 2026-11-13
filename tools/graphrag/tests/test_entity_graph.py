import unittest

from tools.graphrag.ingest.entity_graph import aggregate_source_hash, graph_records, stable_id


class EntityGraphTest(unittest.TestCase):

    def test_ids_and_records_are_deterministic_with_provenance(self):
        record = {
            "dataset": "2wiki", "dataset_release": "r1", "split": "dev",
            "source_hash": "doc-hash", "document_id": "doc1",
            "paragraphs": [{"title": "Alpha", "text": "Alpha has a link."}],
            "evidences": [["Alpha", "related to", "Beta"]],
        }
        chunks = [{"chunk_id": "chunk1", "document_id": "doc1", "text": "Alpha has a link.",
                   "source_hash": "doc-hash"}]
        vertices, edges = graph_records([record], chunks)
        again_vertices, again_edges = graph_records([record], chunks)
        self.assertEqual(vertices, again_vertices)
        self.assertEqual(edges, again_edges)
        self.assertTrue(all(vertex.get("source_document_ids") for vertex in vertices))
        self.assertTrue(all(edge.get("source_document_ids") for edge in edges))
        self.assertTrue(all(vertex.get("split") == "dev" for vertex in vertices))
        self.assertTrue(all(vertex.get("source_hash") for vertex in vertices))
        self.assertTrue(all(edge.get("source_hash") for edge in edges))
        self.assertEqual(stable_id("entity", "2wiki", "r1", "alpha"),
                         next(v["vertex_id"] for v in vertices if v.get("canonical_name") == "Alpha"))

    def test_merged_entity_source_hash_is_order_independent_and_matches_java_contract(self):
        records = [
            {"dataset": "2wiki", "dataset_release": "r1", "split": "dev",
             "source_hash": "hash-a", "document_id": "doc-a",
             "paragraphs": [{"title": "Alpha", "text": "Alpha first."}]},
            {"dataset": "2wiki", "dataset_release": "r1", "split": "dev",
             "source_hash": "hash-b", "document_id": "doc-b",
             "paragraphs": [{"title": "Alpha", "text": "Alpha second."}]},
        ]
        chunks = [
            {"chunk_id": "chunk-a", "document_id": "doc-a", "text": "Alpha first.",
             "source_hash": "hash-a"},
            {"chunk_id": "chunk-b", "document_id": "doc-b", "text": "Alpha second.",
             "source_hash": "hash-b"},
        ]

        vertices, _ = graph_records(records, chunks)
        reversed_vertices, _ = graph_records(list(reversed(records)), chunks)
        alpha = next(value for value in vertices if value.get("canonical_name") == "Alpha")
        reversed_alpha = next(value for value in reversed_vertices
                              if value.get("canonical_name") == "Alpha")

        self.assertEqual(aggregate_source_hash(["hash-a", "hash-b"]), alpha["source_hash"])
        self.assertEqual(alpha["source_hash"], reversed_alpha["source_hash"])
        self.assertEqual(["chunk-a", "chunk-b"], alpha["source_chunk_ids"])

    def test_cooccurrence_edges_are_text_derived_and_provenanced(self):
        record = {
            "dataset": "2wiki", "dataset_release": "r1", "split": "dev",
            "source_hash": "doc-hash", "document_id": "doc1",
            "paragraphs": [
                {"title": "Alpha", "text": "Alpha text."},
                {"title": "Beta", "text": "Beta text."},
                {"title": "Gamma", "text": "Gamma text."},
            ],
        }
        chunks = [{"chunk_id": "chunk1", "document_id": "doc1",
                   "text": "Alpha text. Beta text. Gamma text.",
                   "source_hash": "doc-hash"}]
        _, edges = graph_records([record], chunks)
        cooccurrence = [edge for edge in edges if edge["relation_type"] == "cooccurs"]
        self.assertEqual(3, len(cooccurrence))
        self.assertTrue(all(edge["source_chunk_ids"] == ["chunk1"] for edge in cooccurrence))
        self.assertTrue(all(edge["source_document_ids"] == ["doc1"] for edge in cooccurrence))
        _, repeat_edges = graph_records([record], chunks)
        self.assertEqual(cooccurrence,
                         [edge for edge in repeat_edges if edge["relation_type"] == "cooccurs"])

    def test_supporting_facts_do_not_create_graph_relations(self):
        record = {
            "dataset": "2wiki", "dataset_release": "r1", "split": "dev",
            "source_hash": "doc-hash", "document_id": "doc1",
            "paragraphs": [{"title": "Alpha", "text": "Alpha text."}],
            "supporting_facts": [["Alpha", 0]],
        }
        chunks = [{"chunk_id": "chunk1", "document_id": "doc1", "text": "Alpha text.",
                   "source_hash": "doc-hash"}]
        _, edges = graph_records([record], chunks)
        self.assertEqual([], [edge for edge in edges if edge["relation_type"] != "mentions"])


if __name__ == "__main__":
    unittest.main()
