# Model manifest

Model binaries are provisioned separately from Git. Copy each required file to
the exact path below before starting a node. The startup checks report every
missing runtime model and point back to this document.

| File path | Required by | Purpose | Expected size | SHA-256 |
|---|---|---|---:|---|
| `yolo26n.pt` | A, B, D | YOLO person detection and tracking | 5,544,453 bytes | `9b09cc8bf347f0fc8a5f7657480587f25db09b34bf33b0652110fb03a8ad4fef` |
| `models/reid/person_reid_osnet_x0_25.onnx` | Engine build | Portable OSNet source model | 901,041 bytes | `1d8f60f93a4723da4564d9c8cdf4957540bb9fe62ee92a5bf33feac225566677` |
| `models/reid/person_reid_osnet_x0_25_fp16.engine` | A, B, C, D | TensorRT OSNet Re-ID inference | 1,656,508 bytes | `134d04c85f565739851136a80b149a7da480b697be90c1507501d0a21d62ea6d` |
| `models/face/face_detection_yunet_2023mar.onnx` | A | YuNet face detection | 232,589 bytes | `8f2383e4dd3cfbb4553ea8718107fc0423210dc964f9f4280604804ed2552fa4` |

The TensorRT `.engine` is environment-dependent. Generate or copy it only
between boards with a compatible JetPack, CUDA, TensorRT, GPU architecture,
and TensorRT build configuration. Do not assume an engine built on a desktop
or a different Jetson image is portable.

The repository currently does not automate system package installation or
engine generation. Obtain the approved model bundle from the team, verify it
with `sha256sum`, and place the files at the paths above. The recorded hashes
describe the model files used for the 2026-08 deployment baseline.

`models/face/face_recognition_sface_2021dec.onnx` is used only by optional face
recognition experiments and is not required by node A, B, or D.
