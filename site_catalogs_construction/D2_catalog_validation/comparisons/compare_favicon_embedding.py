"""
Simple favicon similarity using CLIP ViT-B/32.

Usage:
- Single pair: python compare_favicon_embedding.py --img1 path1 --img2 path2
- Demo mode: python compare_favicon_embedding.py
"""

import hashlib
from functools import lru_cache
from pathlib import Path

import numpy as np
from PIL import Image

REPO_ROOT = Path(__file__).resolve().parents[3]
CACHE_DIR = Path(__file__).resolve().parent / ".favicon_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
FAVICONS_ROOT = REPO_ROOT / "data" / "favicons"
SUPPORTED_EXTS = {".ico", ".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".avif"}


def _coerce_clip_feats_tensor(feats, torch_mod):
    if isinstance(feats, tuple):
        feats = feats[0]
    elif hasattr(feats, "pooler_output"):
        feats = feats.pooler_output
    elif hasattr(feats, "last_hidden_state"):
        feats = feats.last_hidden_state.mean(dim=1)
    elif hasattr(feats, "to_tuple"):
        tup = feats.to_tuple()
        if tup:
            feats = tup[0]

    if not isinstance(feats, torch_mod.Tensor):
        raise TypeError(f"Unsupported CLIP output type: {type(feats).__name__}")

    if feats.ndim == 1:
        feats = feats.unsqueeze(0)
    return feats


def _cache_path_for(path):
    digest = hashlib.sha256(str(Path(path).resolve()).encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{digest}.npy"


def load_icon_rgb(path):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Image not found: {p}")

    try:
        img = Image.open(p)
        if img.mode == "P" and "transparency" in img.info:
            img = img.convert("RGBA")
        if img.mode in ("RGBA", "LA"):
            bg = Image.new("RGBA", img.size, (255, 255, 255, 255))
            bg.alpha_composite(img.convert("RGBA"))
            img = bg
        return img.convert("RGB")
    except Exception as exc:
        print(f"[warn] cannot load image {p}: {type(exc).__name__}: {exc}")
        return None


def preprocess_for_clip(img, size=224):
    w, h = img.size
    side = max(w, h)
    canvas = Image.new("RGB", (side, side), (255, 255, 255))
    canvas.paste(img, ((side - w) // 2, (side - h) // 2))
    return canvas.resize((size, size), Image.Resampling.LANCZOS)


def cosine_similarity(v1, v2):
    denom = float(np.linalg.norm(v1) * np.linalg.norm(v2))
    if denom == 0:
        return 0.0
    return float(np.dot(v1, v2) / denom)


@lru_cache(maxsize=1)
def _load_model():
    try:
        import torch
        from transformers import CLIPModel, CLIPProcessor
    except Exception as exc:
        raise RuntimeError(
            "This script requires torch and transformers. "
            "Install with: pip install torch transformers"
        ) from exc

    model_name = "openai/clip-vit-base-patch32"
    model = CLIPModel.from_pretrained(model_name).eval()
    processor = CLIPProcessor.from_pretrained(model_name)
    return torch, model, processor


def embed_icon(path):
    abs_path = str(Path(path).resolve())
    cache_file = _cache_path_for(abs_path)
    if cache_file.exists():
        try:
            return np.load(cache_file)
        except Exception:
            pass

    img = load_icon_rgb(abs_path)
    if img is None:
        return None

    img = preprocess_for_clip(img, size=224)
    torch_mod, model, processor = _load_model()
    inputs = processor(images=img, return_tensors="pt")
    with torch_mod.inference_mode():
        feats = model.get_image_features(**inputs)
    feats = _coerce_clip_feats_tensor(feats, torch_mod)
    vec = feats[0].detach().cpu().numpy().astype(np.float32)

    np.save(cache_file, vec)
    return vec


def batch_embed_icons(paths):
    results = {}
    seen = set()
    for path in paths:
        abs_path = str(Path(path).resolve())
        if abs_path in seen:
            continue
        seen.add(abs_path)
        cache_file = _cache_path_for(abs_path)
        if cache_file.exists():
            try:
                results[abs_path] = np.load(cache_file)
                continue
            except Exception:
                pass

        img = load_icon_rgb(abs_path)
        if img is None:
            continue
        img = preprocess_for_clip(img, size=224)
        embed = embed_icon(abs_path)
        if embed is not None:
            results[abs_path] = embed

    return results


def favicon_similarity_embedding(image_1_path, image_2_path):
    v1 = embed_icon(image_1_path)
    v2 = embed_icon(image_2_path)
    if v1 is None or v2 is None:
        return float("nan")
    return cosine_similarity(v1, v2)


def pick_example_icons(max_icons=6):
    icons = []
    for pattern in ("D1/icons/*", "D2_*/icons/*"):
        for p in sorted(FAVICONS_ROOT.glob(pattern)):
            if p.is_file() and p.suffix.lower() in SUPPORTED_EXTS:
                icons.append(p)
            if len(icons) >= max_icons:
                return icons
    return icons


def main():
    examples = pick_example_icons(6)
    if len(examples) < 2:
        print("Need at least 2 icons under data/favicons")
        return

    print("Example comparisons:")
    shown = 0
    for i in range(len(examples)):
        for j in range(i + 1, len(examples)):
            try:
                score = favicon_similarity_embedding(examples[i], examples[j])
            except Exception as exc:
                print(f"- skipped {examples[i].name} vs {examples[j].name}: {exc}")
                continue
            print(f"- {examples[i].name} vs {examples[j].name}: {score:.4f}")
            shown += 1
            if shown >= 5:
                return


if __name__ == "__main__":
    main()
