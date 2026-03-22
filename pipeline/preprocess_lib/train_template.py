#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fixed Training Script for Preprocessing Experiments

Model is FIXED across all experiments:
- ResGraphSAGE (4-layer)
- Hidden: 128
- Dropout: 0.5
- FocalLoss (alpha=0.85, gamma=2.0)
- Adam (lr=0.005)
- 100 epochs

Only the preprocessed data varies between experiments.

Author: Claude
Date: 2026-01-13
"""

import os
import sys
import json
import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score
from torch_geometric.nn import SAGEConv
from torch.optim import Adam
from tqdm import tqdm

# Import centralized loader factory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from preprocessing_lib.data_loader import (
    create_neighbor_loaders,
    get_default_num_neighbors,
)


class FocalLoss(nn.Module):
    """Focal Loss for imbalanced classification."""

    def __init__(self, alpha=0.85, gamma=2.0, reduction="mean"):
        super().__init__()
        self.alpha = alpha
        self.gamma = gamma
        self.reduction = reduction

    def forward(self, inputs, targets):
        log_prob = F.log_softmax(inputs, dim=1)
        prob = torch.exp(log_prob)
        p_t = prob.gather(1, targets.view(-1, 1)).squeeze()
        alpha_t = torch.where(targets == 1, self.alpha, 1 - self.alpha)
        loss = (
            -alpha_t
            * (1 - p_t) ** self.gamma
            * log_prob.gather(1, targets.view(-1, 1)).squeeze()
        )
        return loss.mean() if self.reduction == "mean" else loss


class ResGraphSAGE(nn.Module):
    """
    Fixed architecture: 4-layer ResGCN with 128 hidden channels.
    """

    def __init__(
        self, input_dim, hidden_dim=128, output_dim=2, num_layers=4, dropout_rate=0.5
    ):
        super().__init__()
        self.num_layers = num_layers
        self.dropout_rate = dropout_rate
        self.convs = nn.ModuleList()
        self.bns = nn.ModuleList()

        self.input_proj = nn.Linear(input_dim, hidden_dim)

        for _ in range(num_layers):
            self.convs.append(SAGEConv(hidden_dim, hidden_dim, aggr="mean"))
            self.bns.append(nn.BatchNorm1d(hidden_dim))

        self.classifier = nn.Linear(hidden_dim, output_dim)

    def forward(self, x, edge_index):
        x = self.input_proj(x)
        x = F.relu(x)
        x = F.dropout(x, p=self.dropout_rate, training=self.training)

        for i, conv in enumerate(self.convs):
            x_in = x
            x = conv(x, edge_index)
            x = self.bns[i](x)
            x = F.relu(x)
            x = F.dropout(x, p=self.dropout_rate, training=self.training)
            x = x + x_in  # Residual connection

        return F.log_softmax(self.classifier(x), dim=1)


def tune_threshold(probs, labels):
    """Find optimal threshold for F1 score."""
    best_threshold, best_f1 = 0.5, 0
    for threshold in np.arange(0.1, 0.95, 0.05):
        preds = (probs > threshold).astype(int)
        f1 = f1_score(labels, preds, zero_division=0)
        if f1 > best_f1:
            best_f1, best_threshold = f1, threshold
    return best_threshold, best_f1


def evaluate(model, loader, device):
    """Evaluate model with GPU accumulation."""
    model.eval()
    all_probs, all_labels = [], []

    with torch.no_grad():
        for batch in loader:
            batch = batch.to(device, non_blocking=True)
            out = model(batch.x, batch.edge_index)[: batch.batch_size]
            probs = torch.exp(out)[:, 1]
            all_probs.append(probs)
            all_labels.append(batch.y[: batch.batch_size])

    all_probs = torch.cat(all_probs).cpu().numpy()
    all_labels = torch.cat(all_labels).cpu().numpy()
    return all_probs, all_labels


def train(data, exp_name):
    """
    Fixed training procedure.

    Args:
        data: PyG Data object (from preprocessing)
        exp_name: Experiment name for logging
    """
    print("\n" + "=" * 60)
    print(f"Training: {exp_name}")
    print("=" * 60)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Device: {device}")

    # FIXED hyperparameters
    num_layers = 4
    hidden_channels = 128
    dropout_rate = 0.5
    learning_rate = 0.005
    max_epochs = 100
    alpha = 0.85
    gamma = 2.0

    # Dynamic batch size from environment (for experiment runner)
    batch_size = int(os.environ.get("BATCH_SIZE", 1024))
    num_neighbors = get_default_num_neighbors(num_layers)  # [10, 10, 5, 5]

    print(f"\nData: {data.num_nodes:,} nodes, {data.num_edges:,} edges")
    print(
        f"Train: {data.train_mask.sum():,}, Val: {data.val_mask.sum():,}, Test: {data.test_mask.sum():,}"
    )
    print(f"Batch size: {batch_size}, Neighbors: {num_neighbors}")

    # Create loaders using centralized factory (optimized settings)
    train_loader, val_loader, test_loader = create_neighbor_loaders(
        data,
        num_layers=num_layers,
        num_neighbors=num_neighbors,
        batch_size=batch_size,
    )

    # Model
    model = ResGraphSAGE(
        data.x.shape[1], hidden_channels, 2, num_layers, dropout_rate
    ).to(device)
    optimizer = Adam(model.parameters(), lr=learning_rate)
    criterion = FocalLoss(alpha=alpha, gamma=gamma).to(device)

    best_val_f1, best_thresh = 0, 0.5

    for epoch in range(1, max_epochs + 1):
        model.train()
        total_loss = 0.0
        total_examples = 0

        # tqdm progress bar for batches
        pbar = tqdm(
            train_loader,
            desc=f"Epoch {epoch:03d}/{max_epochs}",
            leave=False,
            ncols=80,
            bar_format="{desc} |{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        )
        for batch in pbar:
            batch = batch.to(device, non_blocking=True)
            optimizer.zero_grad(set_to_none=True)

            out = model(batch.x, batch.edge_index)[: batch.batch_size]
            loss = criterion(out, batch.y[: batch.batch_size])
            loss.backward()
            optimizer.step()

            total_loss += loss.detach() * batch.batch_size
            total_examples += batch.batch_size

            # Update progress bar postfix
            pbar.set_postfix(loss=f"{loss.item():.4f}")

        avg_loss = (total_loss / total_examples).item()

        # Print epoch summary with [PROGRESS] marker for runner to parse
        print(f"[PROGRESS] Epoch {epoch:03d}/{max_epochs} | Loss: {avg_loss:.4f}")

        if epoch % 5 == 0:
            val_probs, val_labels = evaluate(model, val_loader, device)
            thresh, val_f1 = tune_threshold(val_probs, val_labels)
            print(
                f"[RESULT] Epoch {epoch:03d}: Val F1={val_f1:.4f}, Thresh={thresh:.2f}"
            )

            if val_f1 > best_val_f1:
                best_val_f1, best_thresh = val_f1, thresh

    # Final evaluation
    test_probs, test_labels = evaluate(model, test_loader, device)
    test_preds = (test_probs > best_thresh).astype(int)

    f1 = f1_score(test_labels, test_preds)
    prec = precision_score(test_labels, test_preds, zero_division=0)
    rec = recall_score(test_labels, test_preds, zero_division=0)
    auc = roc_auc_score(test_labels, test_probs)

    print(f"\nTest Results:")
    print(f"  F1: {f1:.4f}")
    print(f"  Precision: {prec:.4f}")
    print(f"  Recall: {rec:.4f}")
    print(f"  AUC: {auc:.4f}")
    print(f"  Threshold: {best_thresh:.2f}")

    # Evaluate on Phase 2 LB (if available)
    from preprocess_lib.train_utils import evaluate_offline_leaderboard

    p2_results = evaluate_offline_leaderboard([model], data, device)

    return {
        "experiment": exp_name,
        "nodes": data.num_nodes,
        "edges": data.num_edges,
        "test_f1": float(f1),
        "test_precision": float(prec),
        "test_recall": float(rec),
        "test_auc": float(auc),
        "best_threshold": float(best_thresh),
        "best_val_f1": float(best_val_f1),
        **p2_results,
    }


def main():
    # Get experiment directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    exp_dir = os.path.dirname(script_dir)
    exp_name = os.path.basename(exp_dir)

    # Load data
    data_path = os.path.join(exp_dir, "data", "processed", "graph_data.pt")
    print(f"Loading: {data_path}")

    if not os.path.exists(data_path):
        print(f"ERROR: Data not found at {data_path}")
        print(
            "Run preprocessing first: python run_all_preprocessing.py --filter <filter_name>"
        )
        sys.exit(1)

    data = torch.load(data_path, weights_only=False)

    # Train
    results = train(data, exp_name)

    # Save results
    output_dir = os.path.join(exp_dir, "outputs")
    os.makedirs(output_dir, exist_ok=True)

    with open(os.path.join(output_dir, "results.json"), "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nResults saved to: {output_dir}/results.json")


if __name__ == "__main__":
    main()
