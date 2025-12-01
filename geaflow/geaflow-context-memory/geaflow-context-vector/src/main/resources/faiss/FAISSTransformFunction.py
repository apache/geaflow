#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Production-ready FAISS Vector Index for GeaFlow Context Memory.
Provides high-performance vector similarity search using Facebook FAISS.
"""

import numpy as np
import faiss
import pickle
from typing import List, Tuple, Dict


class TransFormFunction(object):
    """Base class for GeaFlow-Infer transform functions."""
    
    def __init__(self, input_size):
        self.input_size = input_size
    
    def open(self):
        pass
    
    def process(self, *inputs):
        raise NotImplementedError
    
    def close(self):
        pass


class FAISSTransformFunction(TransFormFunction):
    """
    FAISS-based vector index for similarity search.
    Supports IVF_FLAT, IVF_PQ, and HNSW index types.
    """
    
    def __init__(self, dimension=384, index_type='IVF_FLAT', nlist=100):
        super().__init__(1)
        self.dimension = dimension
        self.index_type = index_type
        self.nlist = nlist
        self.index = None
        self.id_map = {}
        self.next_id = 0
        
    def open(self):
        """Initialize FAISS index."""
        try:
            print(f"[FAISS] Initializing {self.index_type} index, dimension: {self.dimension}")
            
            if self.index_type == 'IVF_FLAT':
                quantizer = faiss.IndexFlatL2(self.dimension)
                self.index = faiss.IndexIVFFlat(quantizer, self.dimension, self.nlist)
                self.index.nprobe = 10
                
            elif self.index_type == 'IVF_PQ':
                quantizer = faiss.IndexFlatL2(self.dimension)
                m = 8
                self.index = faiss.IndexIVFPQ(quantizer, self.dimension, self.nlist, m, 8)
                self.index.nprobe = 10
                
            elif self.index_type == 'HNSW':
                self.index = faiss.IndexHNSWFlat(self.dimension, 32)
                
            elif self.index_type == 'FLAT':
                self.index = faiss.IndexFlatL2(self.dimension)
                
            else:
                raise ValueError(f"Unsupported index type: {self.index_type}")
            
            print(f"[FAISS] Index initialized: {self.index_type}")
            
        except Exception as e:
            print(f"[FAISS] ERROR initializing index: {str(e)}")
            raise e
    
    def process(self, operation, *args):
        """
        Process FAISS operations.
        
        Supported operations:
        - ('add', entity_id, vector): Add vector to index
        - ('search', query_vector, topK, threshold): Search similar vectors
        - ('get', entity_id): Get vector by entity ID
        - ('delete', entity_id): Delete vector
        - ('size',): Get index size
        - ('train', vectors): Train index (for IVF indices)
        """
        if self.index is None:
            raise RuntimeError("Index not initialized")
        
        try:
            op = operation
            
            if op == 'add':
                return self._add_vector(args[0], args[1])
                
            elif op == 'search':
                return self._search(args[0], args[1], args[2])
                
            elif op == 'get':
                return self._get_vector(args[0])
                
            elif op == 'delete':
                return self._delete_vector(args[0])
                
            elif op == 'size':
                return self.index.ntotal
                
            elif op == 'train':
                return self._train(args[0])
                
            else:
                raise ValueError(f"Unknown operation: {op}")
                
        except Exception as e:
            print(f"[FAISS] ERROR during {op}: {str(e)}")
            raise e
    
    def _add_vector(self, entity_id: str, vector: np.ndarray) -> bool:
        """Add vector to index."""
        if not isinstance(vector, np.ndarray):
            vector = np.array(vector, dtype=np.float32)
        
        if vector.shape[0] != self.dimension:
            raise ValueError(f"Vector dimension mismatch: expected {self.dimension}, got {vector.shape[0]}")
        
        vector = vector.reshape(1, -1).astype(np.float32)
        
        if self.index_type.startswith('IVF') and not self.index.is_trained:
            print("[FAISS] WARNING: Index not trained yet, training with single vector")
            self.index.train(vector)
        
        faiss_id = self.next_id
        self.index.add(vector)
        self.id_map[entity_id] = faiss_id
        self.next_id += 1
        
        return True
    
    def _search(self, query_vector: np.ndarray, topK: int, threshold: float) -> List[Tuple[str, float]]:
        """Search for similar vectors."""
        if not isinstance(query_vector, np.ndarray):
            query_vector = np.array(query_vector, dtype=np.float32)
        
        if query_vector.shape[0] != self.dimension:
            raise ValueError(f"Query dimension mismatch: expected {self.dimension}")
        
        query_vector = query_vector.reshape(1, -1).astype(np.float32)
        
        distances, indices = self.index.search(query_vector, topK)
        
        results = []
        reverse_map = {v: k for k, v in self.id_map.items()}
        
        for dist, idx in zip(distances[0], indices[0]):
            if idx == -1:
                continue
            
            similarity = 1.0 / (1.0 + float(dist))
            
            if similarity >= threshold:
                entity_id = reverse_map.get(int(idx), f"unknown_{idx}")
                results.append((entity_id, similarity))
        
        return results
    
    def _get_vector(self, entity_id: str) -> np.ndarray:
        """Get vector by entity ID."""
        if entity_id not in self.id_map:
            return None
        
        faiss_id = self.id_map[entity_id]
        vector = self.index.reconstruct(int(faiss_id))
        
        return vector.astype(np.float32)
    
    def _delete_vector(self, entity_id: str) -> bool:
        """Delete vector (note: FAISS doesn't support deletion, returns False)."""
        if entity_id in self.id_map:
            del self.id_map[entity_id]
            return True
        return False
    
    def _train(self, vectors: np.ndarray) -> bool:
        """Train the index with vectors."""
        if not isinstance(vectors, np.ndarray):
            vectors = np.array(vectors, dtype=np.float32)
        
        if vectors.ndim == 1:
            vectors = vectors.reshape(1, -1)
        
        if self.index_type.startswith('IVF'):
            print(f"[FAISS] Training index with {vectors.shape[0]} vectors")
            self.index.train(vectors.astype(np.float32))
            print("[FAISS] Training complete")
            return True
        
        return False
    
    def close(self):
        """Cleanup resources."""
        if self.index is not None:
            del self.index
            self.index = None
        self.id_map.clear()
        print("[FAISS] Resources cleaned up")


class BatchFAISSTransformFunction(FAISSTransformFunction):
    """
    Batch version for better performance with large datasets.
    """
    
    def add_batch(self, entity_ids: List[str], vectors: np.ndarray) -> bool:
        """Add multiple vectors in batch."""
        if not isinstance(vectors, np.ndarray):
            vectors = np.array(vectors, dtype=np.float32)
        
        if vectors.ndim == 1:
            vectors = vectors.reshape(1, -1)
        
        if self.index_type.startswith('IVF') and not self.index.is_trained:
            print(f"[FAISS] Training with {vectors.shape[0]} vectors")
            self.index.train(vectors)
        
        start_id = self.next_id
        self.index.add(vectors)
        
        for i, entity_id in enumerate(entity_ids):
            self.id_map[entity_id] = start_id + i
        
        self.next_id += len(entity_ids)
        return True
