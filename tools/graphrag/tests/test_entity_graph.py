import unittest

from tools.graphrag.ingest.entity_graph import graph_records, stable_id


class EntityGraphTest(unittest.TestCase):

    def test_ids_and_records_are_deterministic_with_provenance(self):
        record = {
            "dataset": "2wiki", "dataset_release": "r1", "document_id": "doc1",
            "paragraphs": [{"title": "Alpha", "text": "Alpha has a link."}],
            "evidences": [["Alpha", "related to", "Beta"]],
        }
        chunks = [{"chunk_id": "chunk1", "document_id": "doc1", "text": "Alpha has a link."}]
        vertices, edges = graph_records([record], chunks)
        again_vertices, again_edges = graph_records([record], chunks)
        self.assertEqual(vertices, again_vertices)
        self.assertEqual(edges, again_edges)
        self.assertTrue(all(vertex.get("source_document_ids") for vertex in vertices))
        self.assertTrue(all(edge.get("source_document_ids") for edge in edges))
        self.assertEqual(stable_id("entity", "2wiki", "r1", "alpha"),
                         next(v["vertex_id"] for v in vertices if v.get("canonical_name") == "Alpha"))


if __name__ == "__main__":
    unittest.main()
