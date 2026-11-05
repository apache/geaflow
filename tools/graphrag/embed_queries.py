"""Export normalized query embeddings for canonical query JSONL records."""
import argparse
import json
import os
from pathlib import Path

import numpy as np
import torch
from transformers import AutoModel, AutoTokenizer


def _load_encoder(model_path, device):
    dtype = torch.float16 if device.type == "cuda" else torch.float32
    tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
    model = AutoModel.from_pretrained(model_path, local_files_only=True, torch_dtype=dtype)
    model.to(device)
    model.eval()
    return tokenizer, model


def _validate_rows(rows):
    ids = [row.get("id") for row in rows]
    if any(not isinstance(value, str) or not value for value in ids):
        raise ValueError("every query must contain a non-empty id")
    if len(set(ids)) != len(ids):
        raise ValueError("query id values must be unique")
    if any(not isinstance(row.get("question"), str) for row in rows):
        raise ValueError("every query must contain question")


def embed(queries, model_path, batch_size, max_length, device_name="cuda"):
    if batch_size < 1 or max_length < 1:
        raise ValueError("batch-size and max-length must be positive")
    device = torch.device(device_name)
    if device.type == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA device requested but CUDA is unavailable")
    tokenizer, model = _load_encoder(model_path, device)
    vectors = []
    for start in range(0, len(queries), batch_size):
        batch = tokenizer(queries[start:start + batch_size], padding=True, truncation=True,
                          max_length=max_length, return_tensors="pt")
        batch = {key: value.to(device) for key, value in batch.items()}
        with torch.inference_mode():
            hidden = model(**batch).last_hidden_state
            mask = batch["attention_mask"].unsqueeze(-1)
            output = (hidden * mask).sum(dim=1) / mask.sum(dim=1).clamp_min(1)
            output = torch.nn.functional.normalize(output.float(), p=2, dim=1)
        if not torch.isfinite(output).all().item():
            raise ValueError("encoder produced non-finite values")
        norms = torch.linalg.vector_norm(output, dim=1)
        if not torch.allclose(norms, torch.ones_like(norms), atol=1e-3, rtol=1e-3):
            raise ValueError("encoder produced non-normalized vectors")
        vectors.append(output.cpu().numpy())
        print(f"embedded {min(start + batch_size, len(queries))}/{len(queries)}", flush=True)
    if not vectors:
        return np.empty((0, 0), dtype=np.float32)
    result = np.concatenate(vectors).astype(np.float32)
    if result.shape[0] != len(queries) or not np.isfinite(result).all():
        raise ValueError("query vector count or values are invalid")
    return result


def export(rows, model_path, output, batch_size, max_length, device_name="cuda"):
    _validate_rows(rows)
    vectors = embed([row["question"] for row in rows], model_path, batch_size, max_length, device_name)
    if vectors.shape[0] != len(rows):
        raise ValueError("query vector count does not match input count")
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    try:
        with temporary.open("wb") as stream:
            np.savez_compressed(stream, ids=np.asarray([row["id"] for row in rows]), vectors=vectors)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    print(f"wrote {len(rows)} vectors with dimension {vectors.shape[1] if vectors.ndim == 2 else 0} to {target}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queries", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--max-length", type=int, default=512)
    args = parser.parse_args()
    rows = [json.loads(line) for line in Path(args.queries).read_text(encoding="utf-8").splitlines()
            if line.strip()]
    export(rows, args.model, args.output, args.batch_size, args.max_length, args.device)


if __name__ == "__main__":
    main()
