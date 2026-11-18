"""Shared deterministic encoder setup and normalized batch encoding."""

import torch
from transformers import AutoModel, AutoTokenizer


def load_encoder(model_path, device):
    """Load a local-only Hugging Face encoder on the requested device."""
    dtype = torch.float16 if device.type == "cuda" else torch.float32
    tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
    model = AutoModel.from_pretrained(model_path, local_files_only=True, torch_dtype=dtype)
    model.to(device)
    model.eval()
    return tokenizer, model


def encode_batch(texts, tokenizer, model, device, max_length):
    """Mean-pool, L2-normalize, and validate one text batch.

    The returned tensor remains on ``device`` so streaming callers can serialize it directly
    without retaining previous batches in memory.
    """
    if not texts:
        raise ValueError("embedding batch must not be empty")
    encoded = tokenizer(texts, padding=True, truncation=True, max_length=max_length,
                        return_tensors="pt")
    encoded = {key: value.to(device) for key, value in encoded.items()}
    with torch.inference_mode():
        hidden = model(**encoded).last_hidden_state
        mask = encoded["attention_mask"].unsqueeze(-1)
        values = (hidden * mask).sum(dim=1) / mask.sum(dim=1).clamp_min(1)
        values = torch.nn.functional.normalize(values.float(), p=2, dim=1)
    if not torch.isfinite(values).all().item():
        raise ValueError("encoder produced non-finite values")
    norms = torch.linalg.vector_norm(values, dim=1)
    if not torch.allclose(norms, torch.ones_like(norms), atol=1e-3, rtol=1e-3):
        raise ValueError("encoder produced non-normalized vectors")
    return values
