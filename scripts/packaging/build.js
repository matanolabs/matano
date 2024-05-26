const { execSync } = require("child_process");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { exec } = require("pkg");
const crypto = require("crypto");

const randStr = (n = 20) => crypto.randomBytes(n).toString("hex");

const scriptDir = __dirname;
const projDir = path.resolve(scriptDir, "../..");

const baseTempDir = process.env.RUNNER_TEMP || os.tmpdir();
const workDir = fs.mkdtempSync(path.join(baseTempDir, "mtn"));

const workOutputDir = path.resolve(workDir, "build");
execSync(`mkdir -p ${workOutputDir}`);
const cdkPkgConfigPath = path.resolve(workDir, "cdkpkg.json");

process.chdir(workDir);

function getCommitHash() {
  let commitHash = undefined;
  if (process.env.GITHUB_SHA) {
    commitHash = process.env.GITHUB_SHA;
  } else {
    try {
      commitHash = execSync("git rev-parse HEAD").toString();
    } catch (e) {
      // fail locally, no big deal.
    }
  }
  return commitHash?.substring(0, 7);
}

function setCliPackageVersion() {
  const commitHash = getCommitHash();
  if (!commitHash) return;
  const pkgPath = path.resolve(projDir, "cli/package.json");
  const pkg = JSON.parse(fs.readFileSync(pkgPath));
  pkg.version = "0.0.1-" + commitHash;
  fs.writeFileSync(pkgPath, JSON.stringify(pkg, null, 2));
}

function prepareCdkPkg() {
  execSync("npm install aws-cdk@2.85.0", { cwd: workDir });

  // some js template files mess up pkg, just delete
  fs.rmSync(path.resolve(workDir, "node_modules/aws-cdk/lib/init-templates/app/javascript"), {
    recursive: true,
    force: true,
  });
  fs.rmSync(path.resolve(workDir, "node_modules/aws-cdk/lib/init-templates/sample-app/javascript"), {
    recursive: true,
    force: true,
  });

  const cdkPkgConfig = {
    scripts: ["node_modules/aws-cdk/**/*.js"],
    assets: ["node_modules/aws-cdk/**"],
    targets: ["node14.18.1-linux-x64", "node14.18.1-macos-x64"],
    outputPath: workOutputDir,
  };

  fs.writeFileSync(cdkPkgConfigPath, JSON.stringify(cdkPkgConfig));
}

function installMakeself() {
  const url = "https://github.com/megastep/makeself/releases/download/release-2.4.5/makeself-2.4.5.run";
  execSync(`curl -OL ${url}`);
  execSync(`sh ${workDir}/makeself-2.4.5.run --nox11`);
  return `${workDir}/makeself-2.4.5/makeself.sh`;
}

const MShelp = (archiveName) => `\
Installs the Matano CLI

USAGE:
    ./${archiveName} [OPTIONS] [--] [ADDITIONAL OPTIONS]

OPTIONS:
    --target <path>       The directory to install the Matano CLI. By
                              default, this directory is: /usr/local/matano-cli

    (See below for a full list of options)

ADDITIONAL OPTIONS:

    --bin-dir <path>      The directory to store symlinks to executables
                              for the Matano CLI. By default, the directory
                              used is: /usr/local/bin
`;
function getMSHelp(archiveName) {
  const fpath = path.join(baseTempDir, randStr());
  fs.writeFileSync(fpath, MShelp(archiveName));
  return fpath;
}

async function main() {
  execSync(`cp -a ${projDir}/local-assets ${workDir}`);
  const localAssetDir = path.join(workDir, "local-assets");
  const localAssetSubDirs = await fs.promises.readdir(localAssetDir);
  localAssetSubDirs.forEach((subdir) => {
    const subpath = path.join(localAssetDir, subdir);
    execSync(
      `zip -r ${subdir}.zip ./* && cp ${subdir}.zip ${localAssetDir} && cd ${localAssetDir} && rm -rf ${subpath}`,
      {
        cwd: subpath,
      }
    );
  });

  await exec([
    "--no-bytecode",
    "--public",
    "--public-packages",
    "*",
    "-c",
    path.join(projDir, "infra.pkg.json"),
    path.resolve(projDir, "infra/dist/bin/app.js"),
  ]);

  setCliPackageVersion();
  await exec(["--no-bytecode", "--public", "--public-packages", "*", path.resolve(projDir, "cli")]);

  prepareCdkPkg();
  await exec([
    "--no-bytecode",
    "--public",
    "--public-packages",
    "*",
    "-c",
    cdkPkgConfigPath,
    "node_modules/aws-cdk/bin/cdk",
  ]);

  process.chdir(workDir);
  const makeself = installMakeself();

  // TODO: change for arm
  for (const target of ["linux", "macos"]) {
    const targetSubDir = path.join(workOutputDir, target);
    execSync(`mkdir -p ${targetSubDir}`);

    const executables = (await fs.promises.readdir(workOutputDir)).filter((fn) => fn.includes("-" + target));
    for (const executable of executables) {
      const simpleExecutableName = executable
        .replace(`-${target}-x64`, "")
        .replace(`-${target}-arm64`, "")
        .replace(`-${target}`, "");
      execSync(`mv ${path.join(workOutputDir, executable)} ${targetSubDir}/${simpleExecutableName}`);
    }
    execSync(`cp -a ${workDir}/local-assets ${targetSubDir}`);
    execSync(`cp ${path.join(scriptDir, "post-install.sh")} ${targetSubDir}`);

    const archiveName = `matano-${target}-x64.sh`;
    execSync(
      `${makeself} --nochown --help-header ${getMSHelp(
        archiveName
      )} --target /usr/local/matano-cli ${targetSubDir} ${archiveName} "Matano CLI" ./post-install.sh`
    );
    execSync(`chmod 777 ${archiveName}`);

    execSync(`cp ${archiveName} ${projDir}`);

    if (process.env.MTN_GPG_SIGN) {
      console.log("GPG Signing artifact...");
      execSync(`gpg --output ${archiveName}.sig --detach-sig ${archiveName}`);
      execSync(`cp ${archiveName}.sig ${projDir}`);
    }
  }

  execSync(`rm -rf ${workDir}`);
}

main();
