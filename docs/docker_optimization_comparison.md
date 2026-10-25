# Docker Image Optimization

## 1. Key Optimization Techniques

To optimize build performance, reduce image footprint, and enhance security, two key patterns are implemented:

1. **Separation of Builder and Runner (Multi-Stage Build)**:
   - **Builder Stage**: Installs the build toolchain (`gcc`, `g++`, `libpq-dev`), builds wheels, and prepares the Python virtual environment (`/opt/venv`).
   - **Runner Stage**: Uses a minimal `python:3.10-slim` base image with only necessary runtime libraries (`libpq5`). The pre-compiled `/opt/venv` is copied over without carrying heavy compilers or build caches into the production image.

2. **Optimized Layer Caching**:
   - `requirements.txt` is copied and installed **before** copying the application source code.
   - When code changes occur, Docker reuses the cached dependency layer rather than re-downloading and re-installing packages from scratch.

---

## 2. Build Commands

- **Baseline running**:
  ```powershell
  docker build -f docker/Dockerfile.standard -t food_delivery_generator:standard .
  ```

- **Optimized Multi-Stage**:
  ```powershell
  docker build -f docker/Dockerfile -t food_delivery_generator:multistage .
  ```

---

## 3. Optimization Comparison

| Repository / Tag | Image ID | Disk Size | Compressed Size |
| :--- | :--- | :--- | :--- |
| `food_delivery_generator:multistage` | `8cd23cb69caf` | **604MB** | **139MB** |
| `food_delivery_generator:standard` | `ba0e346659f1` | **3.07GB** | **1.04GB** |
