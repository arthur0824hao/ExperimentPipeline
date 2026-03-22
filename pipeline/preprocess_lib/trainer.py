import torch
import torch.nn.functional as F
from torch.utils.tensorboard.writer import SummaryWriter
import tqdm
import json
import os
import numpy as np
import copy
from sklearn.metrics import f1_score
from datetime import datetime


class BaseTrainer:
    def __init__(self, model, optimizer, scheduler, device, exp_name, output_dir="."):
        self.model = model.to(device)
        self.optimizer = optimizer
        self.scheduler = scheduler
        self.device = device
        self.exp_name = exp_name

        self.output_dir = output_dir
        self.ckpt_dir = os.path.join(output_dir, "checkpoints")
        self.res_dir = os.path.join(output_dir, "results_db")

        os.makedirs(self.ckpt_dir, exist_ok=True)
        os.makedirs(self.res_dir, exist_ok=True)

        self.best_f1 = -1
        self.best_epoch = -1
        self.patience_counter = 0

    def get_criterion(self):
        # Default to CrossEntropy with basic weights if not overridden
        # In a real scenario, weights should be calculated from data balance
        return torch.nn.CrossEntropyLoss()

    def train_epoch(self, loader, desc="Train"):
        self.model.train()
        total_loss = 0
        all_preds = []
        all_labels = []

        pbar = tqdm.tqdm(loader, desc=desc, leave=False)
        for batch in loader:
            batch = batch.to(self.device)
            self.optimizer.zero_grad()

            # Assuming batch has x, edge_index, y
            # Some models might need more args, subclasses can override train_epoch
            out = self.model(batch.x, batch.edge_index)

            # Mask handling: if loader provides mask, use it
            # Otherwise assume loader only yields training nodes
            if hasattr(batch, "train_mask") and batch.train_mask is not None:
                loss = self.get_criterion()(
                    out[batch.train_mask], batch.y[batch.train_mask]
                )
                preds = out[batch.train_mask].argmax(dim=1)
                labels = batch.y[batch.train_mask]
            else:
                loss = self.get_criterion()(out, batch.y)
                preds = out.argmax(dim=1)
                labels = batch.y

            loss.backward()
            self.optimizer.step()
            total_loss += loss.item()

            all_preds.append(preds.cpu().numpy())
            all_labels.append(labels.cpu().numpy())
            pbar.set_postfix({"loss": loss.item()})

        if self.scheduler:
            self.scheduler.step()

        all_preds = np.concatenate(all_preds)
        all_labels = np.concatenate(all_labels)
        f1 = f1_score(all_labels, all_preds, average="macro")

        return total_loss / len(loader), f1

    @torch.no_grad()
    def validate(self, loader, desc="Val"):
        self.model.eval()
        total_loss = 0
        all_preds = []
        all_labels = []

        for batch in loader:
            batch = batch.to(self.device)
            out = self.model(batch.x, batch.edge_index)

            loss = self.get_criterion()(out, batch.y)
            preds = out.argmax(dim=1)

            total_loss += loss.item()
            all_preds.append(preds.cpu().numpy())
            all_labels.append(batch.y.cpu().numpy())

        all_preds = np.concatenate(all_preds)
        all_labels = np.concatenate(all_labels)
        f1 = f1_score(all_labels, all_preds, average="macro")

        return total_loss / len(loader), f1

    def train(self, train_loader, val_loader, epochs=100, patience=20):
        print(f"Starting training for {self.exp_name}...")

        results = {
            "exp_name": self.exp_name,
            "config": str(self.optimizer),  # Placeholder for config
            "history": [],
            "best_f1": 0.0,
            "status": "RUNNING",
        }

        for epoch in range(epochs):
            train_loss, train_f1 = self.train_epoch(train_loader, desc=f"Epoch {epoch}")
            val_loss, val_f1 = self.validate(val_loader)

            print(
                f"Epoch {epoch:03d}: Train Loss={train_loss:.4f} F1={train_f1:.4f} | Val Loss={val_loss:.4f} F1={val_f1:.4f}"
            )

            results["history"].append(
                {"epoch": epoch, "train_loss": train_loss, "val_f1": val_f1}
            )

            if val_f1 > self.best_f1:
                self.best_f1 = val_f1
                self.best_epoch = epoch
                self.patience_counter = 0
                self.save_checkpoint("best_model.pt")
                print(f"  * New Best F1: {val_f1:.4f}")
            else:
                self.patience_counter += 1

        results["best_f1"] = self.best_f1
        results["best_epoch"] = self.best_epoch
        results["status"] = "COMPLETED"
        results["completed_at"] = datetime.now().isoformat()

        self.save_results(results)
        print(
            f"Training finished. Best F1: {self.best_f1:.4f} at epoch {self.best_epoch}"
        )

    def save_checkpoint(self, filename):
        path = os.path.join(self.ckpt_dir, f"{self.exp_name}_{filename}")
        torch.save(self.model.state_dict(), path)

    def save_results(self, results):
        path = os.path.join(self.res_dir, f"{self.exp_name}.json")
        with open(path, "w") as f:
            json.dump(results, f, indent=4)

    @torch.no_grad()
    def predict(self, loader, output_file=None):
        self.model.eval()
        # Helper to generate preds for test set
        # This needs customization based on detailed requirements
        pass
