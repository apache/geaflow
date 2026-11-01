"""Export normalized embeddings for canonical chunk JSONL records."""
import argparse
import json
import os
from pathlib import Path

import torch
from transformers import AutoModel, AutoTokenizer


def _load_encoder(model_path, device):
    dtype = torch.float16 if device.type == "cuda" else torch.float32
    tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
    model = AutoModel.from_pretrained(model_path, local_files_only=True, torch_dtype=dtype)
    model.to(device)
    model.eval()
    return tokenizer, model


def _batches(path, batch_size):
    batch = []
    with Path(path).open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"invalid chunk JSON at line {line_number}") from error
            if not isinstance(row, dict) or not isinstance(row.get("chunk_id"), str) \
                    or not row["chunk_id"] or not isinstance(row.get("text"), str):
                raise ValueError(f"invalid chunk record at line {line_number}")
            batch.append(row)
            if len(batch) == batch_size:
                yield batch
                batch = []
    if batch:
        yield batch


def export(chunks_path, model_path, output, device_name, batch_size, max_length):
    if batch_size < 1 or max_length < 1:
        raise ValueError("batch-size and max-length must be positive")
    requested = torch.device(device_name)
    if requested.type == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA device requested but CUDA is unavailable")
    tokenizer, model = _load_encoder(model_path, requested)
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    dimension = None
    written = 0
    seen = set()
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            for batch_rows in _batches(chunks_path, batch_size):
                for row in batch_rows:
                    if row["chunk_id"] in seen:
                        raise ValueError(f"duplicate chunk_id: {row['chunk_id']}")
                    seen.add(row["chunk_id"])
                encoded = tokenizer([row["text"] for row in batch_rows], padding=True,
                                    truncation=True, max_length=max_length, return_tensors="pt")
                encoded = {key: value.to(requested) for key, value in encoded.items()}
                with torch.inference_mode():
                    hidden = model(**encoded).last_hidden_state
                    mask = encoded["attention_mask"].unsqueeze(-1)
                    values = (hidden * mask).sum(dim=1) / mask.sum(dim=1).clamp_min(1)
                    values = torch.nn.functional.normalize(values.float(), p=2, dim=1)
                if not torch.isfinite(values).all().item():
                    raise ValueError("encoder produced non-finite values")
                if dimension is None:
                    dimension = values.shape[1]
                if values.shape[1] != dimension:
                    raise ValueError("encoder produced inconsistent dimensions")
                norms = torch.linalg.vector_norm(values, dim=1)
                if not torch.allclose(norms, torch.ones_like(norms), atol=1e-3, rtol=1e-3):
                    raise ValueError("encoder produced non-normalized vectors")
                for row, vector in zip(batch_rows, values.cpu().tolist()):
                    stream.write(json.dumps({"chunkId": row["chunk_id"], "vector": vector}) + "\n")
                written += len(batch_rows)
                print(f"embedded {written}", flush=True)
            stream.flush()
            os.fsync(stream.fileno())
        if written == 0:
            raise ValueError("no chunks were provided")
        os.replace(temporary, target)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunks", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--max-length", type=int, default=512)
    args = parser.parse_args()
    export(args.chunks, args.model, args.output, args.device, args.batch_size, args.max_length)


if __name__ == "__main__":
    main()
