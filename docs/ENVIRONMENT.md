# Python Environment

## Project standard

- Supported Python: 3.10.x
- Reference environment: Python 3.10.12
- All team members must create this project's virtual environment with Python 3.10.
- Existing system Python installations do not need to be removed.

## Jetson AI nodes

- Python: 3.10.x
- Dependencies: `requirements.txt`
- JetPack-provided CUDA, TensorRT, Torch and OpenCV must not be replaced by generic PyPI builds.

## Windows central server

- Python: 3.10.x
- Dependencies: `requirements-server.txt`

## Django admin

- Python: 3.10.x
- Dependencies will be managed separately or added after integration.

## Important

- Virtual environments are local and must not be committed.
- Switching Git branches does not automatically recreate or change a virtual environment.
- Recreate the environment when the Python major/minor version changes.
- Do not copy a virtual environment between Windows and Jetson.
