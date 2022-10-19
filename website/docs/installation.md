---
sidebar_position: 2
title: Installation
---

Matano always runs in your AWS account. You use the Matano CLI to deploy Matano and manage your installation.

See [Deployment](#) for more on deploying Matano.

### Requirements

- Docker: The Matano CLI requires Docker to be installed.

## Prebuilt installer

Matano provides [a nightly release](https://github.com/matanolabs/matano/releases/tag/nightly) with the latest prebuilt files to install the Matano CLI on GitHub. You can download and execute these files to install Matano.

For example, to install the Matano CLI for Linux, run:

```bash
curl -OL https://github.com/matanolabs/matano/releases/download/nightly/matano-linux-x64.sh
chmod +x matano-linux-x64.sh
sudo ./matano-linux-x64.sh
```

### Customizing the installation

By default, the installer will install Matano in `/usr/local/matano-cli` and create a symlink in `/usr/local/bin`. Thus by default, the installer will require sudo permissions.

You can install without sudo if you specify directories that you already have write permissions to. To customize the installation of Matano, use the `--target` and `--bin-dir` options. For a complete details of these options run the installer with the `--help` flag.

- `--target` – This option specifies the directory to copy all of the files to.

  The default value is `/usr/local/matano-cli`.

- `--bin-dir` – This option specifies that the main `matano` program in the install directory is symbolically linked to the file `matano` in the specified path. You must have write permissions to the specified directory. Creating a symlink to a directory that is already in your path eliminates the need to add the install directory to the user's `$PATH` variable.

  The default value is `/usr/local/bin`.

### (Optional) Verify the installation

The Matano CLI executables are cryptographically signed using PGP signatures. The PGP signatures can be used to verify the validity of the Matano CLI executable. Use the following steps to verify the signatures using the GnuPG tool.

1. Retrieve the Matano PGP public key.

(Option 1) Retrieve the key with the following command.

```bash
gpg --keyserver hkps://keys.openpgp.org --recv c6eee778b33462106f6399103374f1b81a019411
```

(Option 2) Or create a file with the following contents and then import it.

<details><summary>Matano PGP Public Key</summary>

```
-----BEGIN PGP PUBLIC KEY BLOCK-----

mQGNBGMcl30BDACzxi8NnBaZze5uMfsIKQb2m9bsAigQER7SY3XZZLiwBCLx+2cm
4GB1ILA80xn1xzjiM8nZNXRQ9CaHI+3LbtX3xOt26/4ZiAbzIApc9R2hQMby0N6z
yn9aD5VmJlnKJjRshpl8jsFv5od/tdyK990XIMBppxNt3DT9MEK6/epn7ZR61EmL
e4ldLv7RpYwPWuk5ACmFCIZPWfloHVeUZCu/NmLzMJP4qvx7tPoJ+hDGTZ9ILivC
BYe7ZbXhBAbATX+EK+D49EQ0BO74Sp3oTK9oypbP6iIAl4HXritAsJmDxOi/uwDY
4whii+YIRKRgmOKlnDw1L5h+Be/OhNpFCby2d4j/g2AAS1dWY5pNoOfhoErgIeiR
jxiYsO8BIJ8linyQ8orB05fRZbMdjo13827AMnIcJIEaggRBlaU8pVu9prJJryB/
jx9mRzWB2KZuhq83aWkVXb1ZLqPRcOr0tnASctc2wciOtitGuwcG5i3YMAaG9Cy3
yDr2+2FPWIw7Eq8AEQEAAbQgTWF0YW5vIExhYnMgPHN1cHBvcnRAbWF0YW5vLmRl
dj6JAc4EEwEKADgWIQTG7ud4szRiEG9jmRAzdPG4GgGUEQUCYxyXfQIbAwULCQgH
AgYVCgkICwIEFgIDAQIeAQIXgAAKCRAzdPG4GgGUEdleC/oDebqvwUjrJ+bc6BSb
KHKi2rR3vMVkfBsqYsoq9+nogNYVHBzXFLqBoAiI5ztQWV4n1MrWcGgqoN9HyqnM
zBXIuaME92br6UvHaWeHCcjs65VkFy6Jujk6MWjaFty1Kt8H0znTcD/UYBU05VnG
wAptCGwB9GXQk3GR3SFZ/6mx9xxGFud6APZn0XTrLX0939S1BhT8dd7V+Yr6KsTe
gYX1c3e3Qp0E0lpO8E8+UMqou7Nbe/+NU/J3/bO8ywrZ5a1CWasEQkQXsCKOCF2e
9G/tOoZ+B0ko3TlBxUtWNz+yfNSgskto3suVzp/bURqTDBqUvFVPn+seFzPJw87Y
3HQjMehnZLZQOQhg/JidJK0ZRBhLSmkr63vIHzzTdBdwzfTa4/ryql30y/BtogzT
6/LMr9SFIdgsbJRtu/ybdi/ER8CRz07b4l04GBUi7qKW+FDsUqXvAL0yJmK6JeEi
B+JmFae6TVZGFH8ewMzl1jBpvTz2ubet5Ft9u2igoKNu7dK5AY0EYxyXfQEMANlS
ZMeM2D8TN344oDfiSgdKMIRMccMzLlgNFpL9aVP0esy1iYOomhURuPMwrjh+5kRM
d9jml33y9mtLgSJTKOGOX9mG/vWKUxm5s0OrTIVw2mdGHq6mskFbbT8dek4H8CBQ
dJwf6mLcgJQ2H8ulcsvBEM0W8wME2tHFAcnc4+R7O9WLlbVhPvGWaLpe8oNsZMvv
lNTDw7ZyRqv4wY9VHHorMmh93lB9813aRvhettyxYA45CFCFgBRBrnjctgpItJk8
Ha1TSRX6VoEDYufE/GMJ3z0qYqzKQVk6+LB/PxXnYeqdDkpoq+Xc6ocGpDwC0B+R
2Zy/VLqU5YisLGQOosnPNd0QucszvNkPhXilrbS+SEmzAzyW8sOo8xXhKIxsTbw8
mMP6+wIrldJpLn+OHu+q9yyOvKHvCw3MzpcjRWWZvfJdRvFIH5iRKuW6DF2oLLIH
Zy9us7MX4IhVj+NDknDin/9cjhivJR23OPXnk9JFTCfoSigwMPr4FB0aHNBfEwAR
AQABiQG2BBgBCgAgFiEExu7neLM0YhBvY5kQM3TxuBoBlBEFAmMcl30CGwwACgkQ
M3TxuBoBlBE4NQwAgz1gh9eAS2PsxrZI6vty21V4aaAoRUW757ktOfs9ZtHqSEPR
lLhlMnhYO0h00FdG4yVg65Vr93RGKPcIrWHAdZAMn+/GY4NqMBdAHOo0WM3FGIoO
D50LcPE7OwjDOuI8a4bz0jZe29RvET2jmrl4zN8H0tbSzdEp7K+cT/HTjKXyRIGQ
AnVfKFzj50x75+L6OvMU3tkSsVS7SFZUD0cWCekr0LRZKA4D5LAYaMZN57W/7cYj
gWFSED0Re8bcWUSvdRlsEUYvRZ6JWhq4O9E+25fayDky4Xo7vZYZ3I3AYVRuzfrY
yXo2Z1kj3sb/Nt/++NksoE3rxJYqEj3Lf6IiJq8ANHiR4TsbBsngIGIP648DXzwy
wMzhDerSX5DKnkY6lcRoZjYurwEORl4X9N7fE7yoyMkDknRCHtIN49vTF5QcLcEI
pFKd+2Dawz/X7kBWHL8J9murTfhuA0Sl7Zn7FC49gzondVJqZBngbW7qysfUx07z
FXm9MedEAPNT2hc9
=BE9X
-----END PGP PUBLIC KEY BLOCK-----
```

</details>

Import the Matano PGP public key with the following command.

```bash
gpg --import <public_key_filename>
```

2. Download the Matano CLI signatures. The signatures are ASCII detached PGP signatures stored in files with the extension `.sig`. The signatures file has the same name as its corresponding executable, with `.sig` appended.

E.g., for Linux systems, to download the signature file, run:

```bash
curl -OL https://github.com/matanolabs/matano/releases/download/nightly/matano-linux-x64.sh.sig
```

3. Verify the signature with the following command:

```bash
gpg --verify matano-linux-x64.sh.sig matano-linux-x64.sh
```

## Installing from source

You can build the Matano CLI archive installer from source.

```bash
git clone https://github.com/matanolabs/matano.git
cd matano
make package
```

The installer file (e.g. `matano-linux-x64.sh`) will be generated at the project root. You can then execute this file to install Matano.

## Updating the matano CLI

To update the Matano CLI, follow the same instructions as above with an updated installation file.
