"""VLM inference engine: model-agnostic image captioning."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from PIL import Image

from yaruk.vlm.manifest import VLMModelSpec

log = logging.getLogger(__name__)

_CAPTION_PROMPT_TEMPLATE = (
    "You are analyzing a figure from a technical/academic document. "
    "Describe what this figure shows in 2-3 sentences. "
    "Include: the type of figure (graph, circuit diagram, table, photo, etc.), "
    "key elements visible, and what it illustrates. "
    "If there is text or labels, mention them. "
    "Respond in {language}."
)

_LANGUAGE_NAMES = {
    "en": "English",
    "tr": "Turkish",
    "de": "German",
    "fr": "French",
    "es": "Spanish",
    "ja": "Japanese",
    "zh": "Chinese",
    "ko": "Korean",
    "ar": "Arabic",
    "ru": "Russian",
    "pt": "Portuguese",
    "it": "Italian",
}


def _build_prompt(language: str = "en", existing_caption: str = "") -> str:
    lang_name = _LANGUAGE_NAMES.get(language, "English")
    prompt = _CAPTION_PROMPT_TEMPLATE.format(language=lang_name)
    if existing_caption:
        prompt += f"\n\nExisting caption hint: {existing_caption[:200]}"
    return prompt


def generate_caption(
    image_path: Path | str,
    model: Any,
    processor: Any,
    spec: VLMModelSpec,
    language: str = "en",
    existing_caption: str = "",
    max_new_tokens: int = 200,
) -> str | None:
    """Generate a caption for an image using the loaded VLM.

    Returns the generated caption string, or None on failure.
    """

    try:
        img = Image.open(str(image_path)).convert("RGB")
    except Exception as e:
        log.warning("Cannot open image %s: %s", image_path, e)
        return None

    max_dim = 1024
    if max(img.size) > max_dim:
        ratio = max_dim / max(img.size)
        img = img.resize((int(img.width * ratio), int(img.height * ratio)), Image.LANCZOS)

    prompt = _build_prompt(language, existing_caption)

    try:
        if spec.architecture == "qwen3_vl":
            return _infer_qwen_vl(img, prompt, model, processor, max_new_tokens)
        elif spec.architecture == "phi3_vision":
            return _infer_phi_vision(img, prompt, model, processor, max_new_tokens)
        elif spec.architecture == "smolvlm":
            return _infer_smolvlm(img, prompt, model, processor, max_new_tokens)
        else:
            return _infer_generic(img, prompt, model, processor, max_new_tokens)
    except Exception as e:
        log.warning("VLM inference failed for %s with %s: %s", image_path, spec.model_id, e)
        return None


def _infer_qwen_vl(
    img: Image.Image, prompt: str, model: Any, processor: Any, max_tokens: int,
) -> str:
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "image", "image": img},
                {"type": "text", "text": prompt},
            ],
        }
    ]
    text = processor.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    inputs = processor(text=[text], images=[img], return_tensors="pt", padding=True)
    inputs = {k: v.to(model.device) for k, v in inputs.items()}

    import torch
    with torch.inference_mode():
        output_ids = model.generate(**inputs, max_new_tokens=max_tokens, do_sample=False)
    generated = output_ids[0][inputs["input_ids"].shape[1]:]
    return processor.decode(generated, skip_special_tokens=True).strip()


def _infer_phi_vision(
    img: Image.Image, prompt: str, model: Any, processor: Any, max_tokens: int,
) -> str:
    messages = [
        {"role": "user", "content": f"<|image_1|>\n{prompt}"},
    ]
    text = processor.tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    inputs = processor(text, [img], return_tensors="pt")
    inputs = {k: v.to(model.device) for k, v in inputs.items()}

    import torch
    with torch.inference_mode():
        output_ids = model.generate(**inputs, max_new_tokens=max_tokens, do_sample=False)
    generated = output_ids[0][inputs["input_ids"].shape[1]:]
    return processor.decode(generated, skip_special_tokens=True).strip()


def _infer_smolvlm(
    img: Image.Image, prompt: str, model: Any, processor: Any, max_tokens: int,
) -> str:
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "image"},
                {"type": "text", "text": prompt},
            ],
        }
    ]
    text = processor.apply_chat_template(messages, add_generation_prompt=True)
    inputs = processor(text=text, images=[img], return_tensors="pt")
    inputs = {k: v.to(model.device) for k, v in inputs.items()}

    import torch
    with torch.inference_mode():
        output_ids = model.generate(**inputs, max_new_tokens=max_tokens, do_sample=False)
    generated = output_ids[0][inputs["input_ids"].shape[1]:]
    return processor.decode(generated, skip_special_tokens=True).strip()


def _infer_generic(
    img: Image.Image, prompt: str, model: Any, processor: Any, max_tokens: int,
) -> str:
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "image"},
                {"type": "text", "text": prompt},
            ],
        }
    ]
    try:
        text = processor.apply_chat_template(messages, add_generation_prompt=True)
    except Exception:
        text = prompt

    inputs = processor(text=text, images=[img], return_tensors="pt")
    inputs = {k: v.to(model.device) for k, v in inputs.items()}

    import torch
    with torch.inference_mode():
        output_ids = model.generate(**inputs, max_new_tokens=max_tokens, do_sample=False)
    generated = output_ids[0][inputs["input_ids"].shape[1]:]
    return processor.decode(generated, skip_special_tokens=True).strip()


def _validate_caption(caption: str) -> bool:
    """Reject low-quality captions."""
    if not caption or len(caption) < 10:
        return False
    if caption.lower().startswith(("i cannot", "i can't", "sorry", "as an ai")):
        return False
    word_count = len(caption.split())
    return word_count >= 3
