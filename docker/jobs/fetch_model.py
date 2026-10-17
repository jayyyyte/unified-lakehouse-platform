"""Build-time only: download one model at a pinned revision into /opt/models/<org>__<name>.

    python fetch_model.py <org/name> <revision>

Only the files sentence-transformers loads are fetched (the repo also carries ONNX,
OpenVINO, TF and Rust exports, ~10x the size). The revision is written beside the
weights so every refresh can record exactly which model produced its vectors.
"""
import sys

from huggingface_hub import snapshot_download

name, rev = sys.argv[1], sys.argv[2]
target = "/opt/models/" + name.replace("/", "__")
snapshot_download(name, revision=rev, local_dir=target, allow_patterns=[
    "modules.json", "config.json", "config_sentence_transformers.json",
    "sentence_bert_config.json", "special_tokens_map.json", "tokenizer.json",
    "tokenizer_config.json", "vocab.txt", "model.safetensors", "1_Pooling/config.json"])
with open(target + "/REVISION", "w") as f:
    f.write(rev + "\n")
print(f"{name} @ {rev} -> {target}")
