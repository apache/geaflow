#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Production-ready Embedding Transform Function for GeaFlow Context Memory.
Supports Sentence-BERT and other transformer models for text embedding generation.
"""

import torch
import numpy as np
from typing import List, Tuple


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


class EmbeddingTransformFunction(TransFormFunction):
    """
    Sentence-BERT embedding generator for Context Memory.
    Generates 768-dimensional embeddings from text input.
    """
    
    def __init__(self):
        super().__init__(1)
        self.model = None
        self.device = None
        self.model_name = 'sentence-transformers/all-MiniLM-L6-v2'
        self.embedding_dim = 384
    
    def open(self):
        """Initialize the embedding model."""
        try:
            from sentence_transformers import SentenceTransformer
            
            self.device = self._get_device()
            print(f"[EmbeddingTransform] Initializing model: {self.model_name}")
            print(f"[EmbeddingTransform] Using device: {self.device}")
            
            self.model = SentenceTransformer(self.model_name)
            self.model.to(self.device)
            self.model.eval()
            
            print(f"[EmbeddingTransform] Model loaded successfully, dimension: {self.embedding_dim}")
            
        except ImportError as e:
            print(f"[EmbeddingTransform] ERROR: sentence-transformers not installed")
            print(f"[EmbeddingTransform] Please run: pip install sentence-transformers")
            raise e
        except Exception as e:
            print(f"[EmbeddingTransform] ERROR initializing model: {str(e)}")
            raise e
    
    def process(self, text_input):
        """
        Generate embedding for input text.
        
        Args:
            text_input: String or list of strings to embed
            
        Returns:
            numpy array of shape (embedding_dim,) or (batch_size, embedding_dim)
        """
        if self.model is None:
            raise RuntimeError("Model not initialized. Call open() first.")
        
        try:
            if isinstance(text_input, str):
                text_input = [text_input]
            
            with torch.no_grad():
                embeddings = self.model.encode(
                    text_input,
                    convert_to_numpy=True,
                    normalize_embeddings=True,
                    show_progress_bar=False
                )
            
            if len(text_input) == 1:
                return embeddings[0].astype(np.float32)
            else:
                return embeddings.astype(np.float32)
                
        except Exception as e:
            print(f"[EmbeddingTransform] ERROR during embedding: {str(e)}")
            raise e
    
    def close(self):
        """Cleanup resources."""
        if self.model is not None:
            del self.model
            self.model = None
        
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        
        print("[EmbeddingTransform] Resources cleaned up")
    
    def _get_device(self):
        """Determine the best available device."""
        if torch.cuda.is_available():
            return 'cuda'
        elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
            return 'mps'
        else:
            return 'cpu'


class BatchEmbeddingTransformFunction(EmbeddingTransformFunction):
    """
    Batch version of embedding generator for better performance.
    """
    
    def __init__(self, batch_size=32):
        super().__init__()
        self.batch_size = batch_size
    
    def process_batch(self, text_batch: List[str]) -> np.ndarray:
        """
        Process a batch of texts efficiently.
        
        Args:
            text_batch: List of strings to embed
            
        Returns:
            numpy array of shape (batch_size, embedding_dim)
        """
        if self.model is None:
            raise RuntimeError("Model not initialized")
        
        try:
            with torch.no_grad():
                embeddings = self.model.encode(
                    text_batch,
                    batch_size=self.batch_size,
                    convert_to_numpy=True,
                    normalize_embeddings=True,
                    show_progress_bar=False
                )
            
            return embeddings.astype(np.float32)
            
        except Exception as e:
            print(f"[BatchEmbedding] ERROR: {str(e)}")
            raise e
