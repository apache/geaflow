import unittest

import torch

from tools.graphrag.embedding import encode_batch


class _Tokenizer:
    def __call__(self, texts, **kwargs):
        rows = [[float(index + 1)] * (index + 1) for index, _ in enumerate(texts)]
        width = max(map(len, rows))
        input_ids = torch.zeros((len(rows), width), dtype=torch.float32)
        attention_mask = torch.zeros((len(rows), width), dtype=torch.int64)
        for index, row in enumerate(rows):
            input_ids[index, :len(row)] = torch.tensor(row)
            attention_mask[index, :len(row)] = 1
        return {"input_ids": input_ids, "attention_mask": attention_mask}


class _Model:
    def __call__(self, input_ids, **kwargs):
        valid = input_ids > 0
        hidden = torch.stack((input_ids, (~valid).to(input_ids.dtype)), dim=-1)
        return type("ModelOutput", (), {"last_hidden_state": hidden})()


class _NonFiniteModel:
    def __call__(self, input_ids, **kwargs):
        hidden = torch.full((*input_ids.shape, 2), float("nan"))
        return type("ModelOutput", (), {"last_hidden_state": hidden})()


class EmbeddingTest(unittest.TestCase):

    def test_encode_batch_masks_padding_and_returns_unit_vectors(self):
        values = encode_batch(["a", "longer"], _Tokenizer(), _Model(), torch.device("cpu"), 16)

        self.assertEqual((2, 2), tuple(values.shape))
        self.assertTrue(torch.isfinite(values).all().item())
        self.assertTrue(torch.allclose(torch.linalg.vector_norm(values, dim=1),
                                       torch.ones(2), atol=1e-3, rtol=1e-3))
        self.assertTrue(torch.allclose(values, torch.tensor([[1.0, 0.0], [1.0, 0.0]]),
                                       atol=1e-3, rtol=1e-3))

    def test_encode_batch_rejects_empty_input(self):
        with self.assertRaisesRegex(ValueError, "must not be empty"):
            encode_batch([], _Tokenizer(), _Model(), torch.device("cpu"), 16)

    def test_encode_batch_rejects_non_finite_output(self):
        with self.assertRaisesRegex(ValueError, "non-finite"):
            encode_batch(["a"], _Tokenizer(), _NonFiniteModel(), torch.device("cpu"), 16)


if __name__ == "__main__":
    unittest.main()
