# Model manifest

Model binaries are provisioned separately from Git. Copy each required file to
the exact path below before starting a node. Never commit the binaries. The
sizes and SHA-256 values were verified during the 2026-08-18 freeze.

| Logical name | Expected path | Required by | Purpose | Input | Output | Embedding dimension | Runtime requirement | Expected size | SHA-256 |
|---|---|---|---|---|---|---:|---|---:|---|
| YOLO26n | `yolo26n.pt` | A, B, C, D | person detection feeding ByteTrack | image; Ultralytics preprocessing, default inference size | person boxes, confidence, class | n/a | Ultralytics 8.4.112 and compatible PyTorch/CUDA | 5,544,453 bytes | `9b09cc8bf347f0fc8a5f7657480587f25db09b34bf33b0652110fb03a8ad4fef` |
| OSNet x0.25 ONNX | `models/reid/person_reid_osnet_x0_25.onnx` | engine build only | portable source used to build the TensorRT engine | float32 NCHW `[batch,3,256,128]`, RGB normalized | `[batch,512]`, L2-normalized | 512 | ONNX tooling is needed only for engine build, not node runtime | 901,041 bytes | `1d8f60f93a4723da4564d9c8cdf4957540bb9fe62ee92a5bf33feac225566677` |
| OSNet x0.25 FP16 TensorRT | `models/reid/person_reid_osnet_x0_25_fp16.engine` | A, B, C, D | body Re-ID inference | FP16-compatible NCHW `[1,3,256,128]`, RGB normalized | `[1,512]`, L2-normalized | 512 | compatible JetPack, CUDA, TensorRT, GPU architecture, and PyTorch CUDA stream | 1,656,508 bytes | `134d04c85f565739851136a80b149a7da480b697be90c1507501d0a21d62ea6d` |
| YuNet 2023mar | `models/face/face_detection_yunet_2023mar.onnx` | A | face box, five landmarks, and score | BGR image; input size set to the current crop size | 15-value face rows: box, landmarks, score | n/a | OpenCV DNN with `FaceDetectorYN` | 232,589 bytes | `8f2383e4dd3cfbb4553ea8718107fc0423210dc964f9f4280604804ed2552fa4` |
| SFace 2021dec | `models/face/face_recognition_sface_2021dec.onnx` | A | aligned face Re-ID feature | aligned face crop from `FaceRecognizerSF.alignCrop` | float feature vector | 128 | OpenCV DNN with `FaceRecognizerSF` | 38,696,353 bytes | `0ba9fbfa01b5270c96627c4ef784da859931e02f04419c829e83484087c34e79` |

The TensorRT `.engine` is environment-dependent. Generate or copy it only
between boards with a compatible JetPack, CUDA, TensorRT, GPU architecture,
and TensorRT build configuration. Do not assume an engine built on a desktop
or a different Jetson image is portable.

The repository does not automate system package installation or engine
generation. Obtain the approved model bundle from the team, verify it with
`sha256sum`, and place each file at the path above. The SFace model is a required
Camera A runtime model: `src.nodes.node_a` checks it at startup and uses it to
produce 128-D face embeddings.
