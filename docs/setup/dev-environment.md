# Development environment setup (Windows 10 + WSL2)

Implements [ADR-0001](../adr/0001-development-environment.md). Target host: Windows 10 Home 22H2 (build 19045), 7.4 GB RAM, 8 cores, firmware virtualisation **enabled** (checked), 131 GB free disk.

Sources checked on 2026-09-25: Microsoft Learn *Advanced settings configuration in WSL* (updated 2026-09-16) and Docker *Install Docker Engine on Ubuntu*.

> **Order matters.** `sparseVhd` and `defaultVhdSize` only apply to virtual disks created *after* `.wslconfig` exists. Install the WSL platform first, write `.wslconfig`, and only then install Ubuntu.

---

## Step 1 — Install the WSL platform (no distribution yet)

PowerShell **as Administrator**:

```powershell
wsl --install --no-distribution
```

**Reboot.**

If the command only prints usage help (old inbox WSL), enable the components manually, reboot, then update:

```powershell
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
# reboot, then:
wsl --update
wsl --set-default-version 2
```

Verify (normal PowerShell):

```powershell
wsl --version     # must print a WSL version (Store WSL); if not recognised, run `wsl --update`
```

## Step 2 — `.wslconfig` (before installing Ubuntu)

Create `C:\Users\<windows-user>\.wslconfig`:

```ini
# Global WSL 2 settings — sized for a 7.4 GB / 8-core host.
[wsl2]
# Hard cap for the Linux VM. ~2.4 GB stay for Windows + VS Code UI + browser.
memory=5GB
# Leave 2 cores to Windows so the desktop stays responsive during Spark runs.
processors=6
# Safety net against OOM kills during stress tests; slow, not something to rely on.
swap=4GB
# Cap the distro's virtual disk (Docker images + ivy/coursier caches + lake data).
defaultVhdSize=80GB

[experimental]
# Return page cache to Windows when idle (documented default; explicit on purpose).
autoMemoryReclaim=dropCache
# The VHD shrinks when files are deleted. Applies to VHDs created after this file exists.
sparseVhd=true
```

`networkingMode=mirrored` and `[wsl2]` keys marked "Windows 11 only" are deliberately absent.

## Step 3 — Install Ubuntu

```powershell
wsl --install -d Ubuntu-24.04
```

Create your Linux user when prompted. Then, from PowerShell:

```powershell
wsl -l -v          # Ubuntu-24.04 · VERSION 2
```

Ubuntu 26.04 LTS also works. 24.04 is chosen for maturity with the JVM and Docker tooling.

## Step 4 — Base packages and `wsl.conf`

Inside Ubuntu:

```bash
sudo apt update && sudo apt full-upgrade -y
sudo apt install -y build-essential curl git zip unzip ca-certificates

sudo tee /etc/wsl.conf >/dev/null <<'EOF'
[boot]
systemd=true

[interop]
# Keep Windows' PATH (with its Java 8/17) out of Linux, so tools resolve to Linux binaries.
appendWindowsPath=false
EOF
```

From PowerShell: `wsl --shutdown`, wait about 8 seconds, reopen Ubuntu. Verify: `systemctl is-system-running` should print `running` or `degraded`.

> Microsoft's page lists the `[boot]` section as Windows 11 / Server 2022. On Windows 10 with the Store version of WSL, systemd usually works. **If `systemctl` reports that systemd is not running**, Docker will not auto-start: use `sudo service docker start` once per WSL session (step 5).

Because `appendWindowsPath=false`, add VS Code's CLI manually to `~/.bashrc`:

```bash
echo 'export PATH="$PATH:/mnt/c/Users/<windows-user>/AppData/Local/Programs/Microsoft VS Code/bin"' >> ~/.bashrc
```

## Step 5 — Docker Engine (inside WSL, no Docker Desktop)

The official apt repository, verbatim from docs.docker.com:

```bash
sudo apt update
sudo apt install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

sudo tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF

sudo apt update
sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

Post-install:

```bash
sudo usermod -aG docker "$USER"     # run docker without sudo (the docker group is root-equivalent; fine on a personal laptop)
sudo systemctl enable --now docker  # with systemd; otherwise: sudo service docker start
# close and reopen the Ubuntu shell so the group change applies, then:
docker run --rm hello-world
docker compose version
```

Testcontainers finds the daemon through `/var/run/docker.sock`; nothing else to configure.

## Step 6 — JDK 17 and sbt (SDKMAN)

```bash
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk list java | grep -- "-tem" | grep " 17\."   # pick the latest 17.x Temurin identifier
sdk install java <17.x.y-tem>
sdk install sbt
java -version          # 17.x, Temurin
sbt --script-version
```

The repository will pin the JDK in `.sdkmanrc` (`sdk env` switches to it) and sbt in `project/build.properties` (P1).

## Step 7 — Git

```bash
git config --global user.name  "Miguel"
git config --global user.email "<the email you use for GitHub>"
git config --global init.defaultBranch main
git config --global core.autocrlf input      # LF in the repo; never CRLF from Linux
ssh-keygen -t ed25519 -C "<email>"           # add ~/.ssh/id_ed25519.pub to GitHub once the account exists
```

## Step 8 — Move the repository into the WSL filesystem

The repository currently lives in `C:\Users\<windows-user>\<folder>\ingestion-framework`. Clone it into WSL so that history comes along and the Windows copy stays as a backup:

```bash
mkdir -p ~/code
git clone "/mnt/c/Users/<windows-user>/<folder>/ingestion-framework" ~/code/ingestion-framework
cd ~/code/ingestion-framework
git remote remove origin       # the Windows copy is not the remote; GitHub will be, later
```

The final directory name follows the repository name chosen in D6. Renaming the folder later is harmless.

**Line endings.** The initial commit was made on Windows with `core.autocrlf=true`. P1 adds a `.gitattributes` (`* text=auto eol=lf`) and renormalises, so the repository stores LF from then on.

## Step 9 — VS Code + Metals

1. On Windows, install the VS Code extension **WSL** (`ms-vscode-remote.remote-wsl`).
2. In Ubuntu: `cd ~/code/ingestion-framework && code .` → VS Code reopens connected to WSL (the VS Code Server installs itself the first time).
3. In that WSL window, install **Scala (Metals)** (`scalameta.metals`). It will offer to import the sbt build once P1 creates it.
4. Keep Metals lean on this machine. In the WSL window's settings (`settings.json`):
   ```json
   { "metals.serverProperties": ["-Xmx768m", "-XX:+UseG1GC"] }
   ```
   Stop Metals (*Metals: Stop server*) before stress tests or Standalone demos (discovery §7.3).

## Step 10 — Verification checklist

Run inside Ubuntu and paste the output when starting P1:

```bash
free -h                         # Mem total ≈ 4.8Gi, Swap 4.0Gi
nproc                           # 6
systemctl is-system-running     # running / degraded (or: systemd not in use → manual docker start)
docker version --format '{{.Server.Version}}' && docker compose version
java -version
sbt --script-version
git --version
pwd                             # /home/<user>/code/…, NOT /mnt/c/…
```

Not needed until P11 / E1: a Spark 4.1.3 binary distribution for Standalone demos. Development and tests download Spark as sbt dependencies.
