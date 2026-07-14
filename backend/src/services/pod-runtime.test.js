import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test, { afterEach } from "node:test";

import {
  __testing,
  destroyPod,
  ensurePod,
  podExec,
  podInspect,
  podName,
  podNetworkName
} from "./pod-runtime.js";

const temporaryRoots = [];

afterEach(async () => {
  __testing.reset();
  await Promise.all(temporaryRoots.splice(0).map((root) => fs.rm(root, { recursive: true, force: true })));
});

function projectSubnet(project, offset = 0) {
  const slots = 8192;
  const first = crypto.createHash("sha256").update(project, "utf8").digest().readUInt32BE(0) % slots;
  const index = (first + offset) % slots;
  const address = 0x0af00000 + index * 8;
  return [
    Math.floor(address / 2 ** 24),
    Math.floor(address / 2 ** 16) % 256,
    Math.floor(address / 2 ** 8) % 256,
    address % 256
  ].join(".") + "/29";
}

class FakeDocker {
  constructor() {
    this.calls = [];
    this.networks = new Map();
    this.containers = new Map();
    this.runGate = null;
    this.nextIp = 2;
    this.hostProjectsRoot = "";
    this.failNextNetworkCreateWithOverlap = false;
    this.commandGate = null;
  }

  add(project, overrides = {}) {
    const name = podName(project);
    const networkName = overrides.networkName || "reaper-net";
    this.containers.set(name, {
      project,
      name,
      running: overrides.running ?? false,
      id: overrides.id || `container-${project}`,
      image: overrides.image || "reaper-pod:latest",
      workingDir: overrides.workingDir || "/work",
      bind: overrides.bind,
      restart: overrides.restart || "unless-stopped",
      memory: overrides.memory ?? 8 * 1024 ** 3,
      memorySwap: overrides.memorySwap ?? 8 * 1024 ** 3,
      nanoCpus: overrides.nanoCpus ?? 4_000_000_000,
      pidsLimit: overrides.pidsLimit ?? 4096,
      capDrop: overrides.capDrop ?? ["NET_RAW"],
      securityOpt: overrides.securityOpt ?? ["no-new-privileges:true"],
      networks: new Map([[networkName, overrides.ip ?? "10.77.1.99"]])
    });
  }

  addNetwork(project, overrides = {}) {
    const name = overrides.name || podNetworkName(project);
    this.networks.set(name, {
      driver: overrides.driver || "bridge",
      icc: overrides.icc ?? "false",
      project: overrides.project ?? project,
      subnet: overrides.subnet || projectSubnet(project)
    });
    return name;
  }

  delayNextRun() {
    let release;
    this.runGate = new Promise((resolve) => { release = resolve; });
    return release;
  }
  delayCommands() {
    let release;
    this.commandGate = new Promise((resolve) => { release = resolve; });
    return release;
  }

  inspectJson(container) {
    return JSON.stringify([{
      Id: container.id,
      Name: `/${container.name}`,
      State: { Running: container.running },
      Config: {
        Image: container.image,
        WorkingDir: container.workingDir,
        Labels: { "reaper.project": container.project }
      },
      HostConfig: {
        Binds: [container.bind || `${path.join(this.hostProjectsRoot, container.project)}:/work`],
        RestartPolicy: { Name: container.restart },
        Memory: container.memory,
        MemorySwap: container.memorySwap,
        NanoCpus: container.nanoCpus,
        PidsLimit: container.pidsLimit,
        CapDrop: container.capDrop,
        SecurityOpt: container.securityOpt
      },
      NetworkSettings: {
        Networks: Object.fromEntries(
          [...container.networks].map(([name, ip]) => [name, { IPAddress: container.running ? ip : "" }])
        )
      }
    }]);
  }

  assignIp() {
    return `172.30.${this.nextIp++}.2`;
  }

  run = async (file, args, options = {}) => {
    assert.equal(file, "docker");
    this.calls.push({ args: [...args], options: { ...options } });
    if (this.commandGate) await this.commandGate;

    if (args[0] === "version") return { code: 0, stdout: "Docker fake", stderr: "" };
    if (args[0] === "network" && args[1] === "ls") {
      return { code: 0, stdout: `${[...this.networks.keys()].join("\n")}\n`, stderr: "" };
    }
    if (args[0] === "network" && args[1] === "inspect") {
      const names = args.slice(2);
      const missing = names.find((name) => !this.networks.has(name));
      if (missing) return { code: 1, stdout: "", stderr: "Error: No such network" };
      return {
        code: 0,
        stdout: JSON.stringify(names.map((name) => {
          const network = this.networks.get(name);
          return {
            Name: name,
            Driver: network.driver,
            Options: { "com.docker.network.bridge.enable_icc": network.icc },
            Labels: { "reaper.project": network.project },
            IPAM: { Config: [{ Subnet: network.subnet }] }
          };
        })),
        stderr: ""
      };
    }
    if (args[0] === "network" && args[1] === "create") {
      const name = args.at(-1);
      const subnet = args[args.indexOf("--subnet") + 1];
      if (this.failNextNetworkCreateWithOverlap) {
        this.failNextNetworkCreateWithOverlap = false;
        this.networks.set("racing-foreign-network", {
          driver: "bridge", icc: "true", project: "foreign", subnet
        });
        return { code: 1, stdout: "", stderr: "Pool overlaps with other one on this address space" };
      }
      const label = args[args.indexOf("--label") + 1];
      this.networks.set(name, {
        driver: args[args.indexOf("--driver") + 1],
        icc: args[args.indexOf("--opt") + 1].split("=")[1],
        project: label.slice("reaper.project=".length),
        subnet
      });
      return { code: 0, stdout: "network-id", stderr: "" };
    }
    if (args[0] === "network" && args[1] === "connect") {
      const container = this.containers.get(args[3]);
      container.networks.set(args[2], this.assignIp());
      return { code: 0, stdout: "", stderr: "" };
    }
    if (args[0] === "network" && args[1] === "disconnect") {
      this.containers.get(args[3]).networks.delete(args[2]);
      return { code: 0, stdout: "", stderr: "" };
    }
    if (args[0] === "network" && args[1] === "rm") {
      this.networks.delete(args[2]);
      return { code: 0, stdout: args[2], stderr: "" };
    }
    if (args[0] === "inspect") {
      const container = this.containers.get(args[1]);
      return container
        ? { code: 0, stdout: this.inspectJson(container), stderr: "" }
        : { code: 1, stdout: "", stderr: "Error: No such object: container" };
    }
    if (args[0] === "run") {
      if (this.runGate) {
        const gate = this.runGate;
        this.runGate = null;
        await gate;
      }
      const name = args[args.indexOf("--name") + 1];
      const label = args[args.indexOf("--label") + 1];
      const project = label.slice("reaper.project=".length);
      const networkName = args[args.indexOf("--network") + 1];
      const bind = args[args.indexOf("-v") + 1];
      this.containers.set(name, {
        project, name, running: true, id: "container-id",
        image: args.at(-1), workingDir: args[args.indexOf("-w") + 1], bind,
        restart: args[args.indexOf("--restart") + 1],
        memory: 8 * 1024 ** 3, memorySwap: 8 * 1024 ** 3,
        nanoCpus: 4_000_000_000, pidsLimit: 4096,
        capDrop: [args[args.indexOf("--cap-drop") + 1]],
        securityOpt: [args[args.indexOf("--security-opt") + 1]],
        networks: new Map([[networkName, this.assignIp()]])
      });
      return { code: 0, stdout: "container-id", stderr: "" };
    }
    if (args[0] === "update") {
      const container = this.containers.get(args.at(-1));
      if (args.includes("--restart")) container.restart = args[args.indexOf("--restart") + 1];
      if (args.includes("--memory")) container.memory = 8 * 1024 ** 3;
      if (args.includes("--memory-swap")) container.memorySwap = 8 * 1024 ** 3;
      if (args.includes("--cpus")) container.nanoCpus = 4_000_000_000;
      if (args.includes("--pids-limit")) container.pidsLimit = 4096;
      return { code: 0, stdout: container.name, stderr: "" };
    }
    if (args[0] === "start") {
      this.containers.get(args[1]).running = true;
      return { code: 0, stdout: args[1], stderr: "" };
    }
    if (args[0] === "stop") {
      this.containers.get(args[1]).running = false;
      return { code: 0, stdout: args[1], stderr: "" };
    }
    if (args[0] === "rm") {
      this.containers.delete(args[1]);
      return { code: 0, stdout: args[1], stderr: "" };
    }
    if (args[0] === "exec") {
      return { code: 7, stdout: "captured stdout", stderr: "captured stderr" };
    }
    throw new Error(`unexpected fake docker call: ${args.join(" ")}`);
  };
}

async function setup(fake = new FakeDocker(), existingRoot) {
  const root = existingRoot || await fs.mkdtemp(path.join(os.tmpdir(), "reaper-pods-test-"));
  if (!existingRoot) temporaryRoots.push(root);
  const projectsRoot = path.join(root, "backend-projects");
  const hostProjectsRoot = path.join(root, "host-projects");
  const stateDir = path.join(root, "state");
  await fs.mkdir(projectsRoot, { recursive: true });
  fake.hostProjectsRoot = hostProjectsRoot;
  __testing.configure({
    commandRunner: fake.run,
    projectsRoot,
    hostProjectsRoot,
    stateDir
  });
  return { fake, root, projectsRoot, hostProjectsRoot, stateDir };
}

function projectPath(context, project) {
  return path.join(context.projectsRoot, project);
}

test("pod and private network names are deterministic and collision-resistant", () => {
  const mixed = podName("My_Project.v2");
  assert.match(mixed, /^reaper-pod-my-project-v2-[a-f0-9]{10}$/);
  assert.equal(mixed, podName("My_Project.v2"));
  assert.match(podNetworkName("My_Project.v2"), /^reaper-net-my-project-v2-[a-f0-9]{10}$/);
  assert.notEqual(podNetworkName("foo_bar"), podNetworkName("foo-bar"));
  assert.match(podName("mimo playground"), /^reaper-pod-mimo-playground-[a-f0-9]{10}$/);
  for (const invalid of ["", ".", "..", "../project", "project/child", "project\\child", "bad\0name"]) {
    assert.throws(() => podName(invalid), /invalid project name/);
    assert.throws(() => podNetworkName(invalid), /invalid project name/);
  }
});

test("new pods use only a labelled ICC-disabled bridge with Docker-assigned IP and security flags", async () => {
  const context = await setup();
  const result = await ensurePod("alpha", projectPath(context, "alpha"));
  const create = context.fake.calls.find(({ args }) => args[0] === "network" && args[1] === "create");
  assert.deepEqual(create.args, [
    "network", "create", "--driver", "bridge", "--subnet", projectSubnet("alpha"),
    "--opt", "com.docker.network.bridge.enable_icc=false",
    "--label", "reaper.project=alpha", podNetworkName("alpha")
  ]);
  const run = context.fake.calls.find(({ args }) => args[0] === "run");
  assert.equal(run.args.includes("--ip"), false);
  assert.equal(run.args[run.args.indexOf("--network") + 1], podNetworkName("alpha"));
  assert.ok(run.args.includes("NET_RAW"));
  assert.ok(run.args.includes("no-new-privileges"));
  assert.deepEqual(result, {
    name: podName("alpha"),
    ip: "172.30.2.2",
    generation: "container-id",
    legacySecurity: false
  });
});

test("subnet probing skips occupied deterministic slots and retries a create race", async () => {
  const fake = new FakeDocker();
  fake.addNetwork("foreign", {
    name: "preexisting-foreign-network",
    project: "foreign",
    subnet: projectSubnet("alpha")
  });
  fake.failNextNetworkCreateWithOverlap = true;
  const context = await setup(fake);

  await ensurePod("alpha", projectPath(context, "alpha"));
  const creates = fake.calls.filter(({ args }) => args[0] === "network" && args[1] === "create");
  assert.equal(creates.length, 2);
  assert.equal(creates[0].args[creates[0].args.indexOf("--subnet") + 1], projectSubnet("alpha", 1));
  assert.equal(creates[1].args[creates[1].args.indexOf("--subnet") + 1], projectSubnet("alpha", 2));
  assert.equal(fake.networks.get(podNetworkName("alpha")).subnet, projectSubnet("alpha", 2));
});

test("subnet probing rejects overlap with a broader existing Docker network", async () => {
  const fake = new FakeDocker();
  const candidate = projectSubnet("broad-overlap");
  const containingSubnet = `${candidate.split(".").slice(0, 3).join(".")}.0/24`;
  fake.addNetwork("foreign", {
    name: "broad-foreign-network",
    project: "foreign",
    subnet: containingSubnet
  });
  const context = await setup(fake);

  await ensurePod("broad-overlap", projectPath(context, "broad-overlap"));
  const createdSubnet = fake.networks.get(podNetworkName("broad-overlap")).subnet;
  assert.notEqual(createdSubnet, candidate);
  assert.notEqual(createdSubnet.split(".").slice(0, 3).join("."), candidate.split(".").slice(0, 3).join("."));
});

test("foreign or insecure private-network collisions fail closed", async () => {
  for (const overrides of [
    { driver: "overlay" },
    { icc: "true" },
    { project: "intruder" },
    { subnet: projectSubnet("alpha", 1) }
  ]) {
    const fake = new FakeDocker();
    fake.addNetwork("alpha", overrides);
    const context = await setup(fake);
    await assert.rejects(
      ensurePod("alpha", projectPath(context, "alpha")),
      /labelled bridge.*ICC disabled.*exact subnet/
    );
    assert.equal(fake.calls.some(({ args }) => args[0] === "run"), false);
  }
});

test("running owned pod is live-migrated and extra networks are disconnected without stop or rm", async () => {
  const fake = new FakeDocker();
  fake.add("alpha", { running: true, capDrop: [], securityOpt: [] });
  fake.containers.get(podName("alpha")).networks.set("shared-extra", "10.88.0.7");
  const context = await setup(fake);

  const result = await ensurePod("alpha", projectPath(context, "alpha"));
  const lifecycle = fake.calls
    .filter(({ args }) => args[0] === "network" || ["start", "stop", "rm"].includes(args[0]))
    .map(({ args }) => args.slice(0, 3));
  assert.ok(lifecycle.some((args) => args[0] === "network" && args[1] === "connect"));
  assert.ok(lifecycle.some((args) => args[0] === "network" && args[1] === "disconnect" && args[2] === "reaper-net"));
  assert.ok(lifecycle.some((args) => args[0] === "network" && args[1] === "disconnect" && args[2] === "shared-extra"));
  assert.equal(fake.calls.some(({ args }) => ["start", "stop", "rm"].includes(args[0])), false);
  assert.deepEqual([...fake.containers.get(podName("alpha")).networks.keys()], [podNetworkName("alpha")]);
  assert.equal(result.legacySecurity, true);
  assert.match(result.ip, /^172\.30\./);
});

test("stopped owned pod is attached and isolated before it is started", async () => {
  const fake = new FakeDocker();
  fake.add("alpha", { running: false });
  const context = await setup(fake);

  const result = await ensurePod("alpha", projectPath(context, "alpha"));
  const connectIndex = fake.calls.findIndex(({ args }) => args[0] === "network" && args[1] === "connect");
  const disconnectIndex = fake.calls.findIndex(({ args }) => args[0] === "network" && args[1] === "disconnect");
  const startIndex = fake.calls.findIndex(({ args }) => args[0] === "start");
  assert.ok(connectIndex >= 0 && disconnectIndex > connectIndex && startIndex > disconnectIndex);
  assert.equal(fake.calls.some(({ args }) => args[0] === "run" || args[0] === "rm"), false);
  assert.equal(result.ip, "172.30.2.2");
  assert.equal(result.legacySecurity, false);
});

test("mutable restart and resource drift is updated and re-inspected", async () => {
  const fake = new FakeDocker();
  fake.addNetwork("alpha");
  fake.add("alpha", {
    running: true,
    networkName: podNetworkName("alpha"),
    restart: "no",
    memory: 1024,
    memorySwap: 2048,
    nanoCpus: 1_000_000_000,
    pidsLimit: 100
  });
  const context = await setup(fake);

  const result = await ensurePod("alpha", projectPath(context, "alpha"));
  const update = fake.calls.find(({ args }) => args[0] === "update");
  assert.deepEqual(update.args, [
    "update", "--restart", "unless-stopped",
    "--memory", "8g", "--memory-swap", "8g",
    "--cpus", "4", "--pids-limit", "4096", podName("alpha")
  ]);
  assert.equal(result.legacySecurity, false);
});

test("owner, bind, workdir, and image drift fail closed without mutating the pod", async () => {
  const cases = [
    ["owner", (container) => { container.project = "intruder"; }, /pod name collision/],
    ["bind", (container) => { container.bind = "/wrong:/work"; }, /workspace bind/],
    ["workdir", (container) => { container.workingDir = "/tmp"; }, /working directory/],
    ["image", (container) => { container.image = "foreign:latest"; }, /unexpected image/]
  ];
  for (const [, mutate, expected] of cases) {
    const fake = new FakeDocker();
    fake.addNetwork("alpha");
    fake.add("alpha", { running: true, networkName: podNetworkName("alpha") });
    mutate(fake.containers.get(podName("alpha")));
    const context = await setup(fake);
    await assert.rejects(ensurePod("alpha", projectPath(context, "alpha")), expected);
    assert.equal(fake.calls.some(({ args }) => ["update", "start", "stop", "rm"].includes(args[0])), false);
  }
});

test("podInspect selects the private-network IP and reports legacy security", async () => {
  const fake = new FakeDocker();
  fake.add("alpha", { running: true, capDrop: [], securityOpt: [] });
  const container = fake.containers.get(podName("alpha"));
  container.networks.set(podNetworkName("alpha"), "172.31.9.4");
  await setup(fake);
  assert.deepEqual(await podInspect("alpha"), {
    exists: true,
    running: true,
    ip: "172.31.9.4",
    isolated: false,
    generation: "container-alpha",
    legacySecurity: true
  });
});

test("new pod security check accepts Docker API CAP_-prefixed capabilities", async () => {
  const fake = new FakeDocker();
  fake.addNetwork("alpha");
  const context = await setup(fake);
  // Modern Docker Engine reports `--cap-drop NET_RAW` as `CAP_NET_RAW` in
  // the post-create inspect. Override the fake's inspect to simulate that.
  const original = fake.inspectJson.bind(fake);
  fake.inspectJson = (container) => {
    const json = JSON.parse(original(container));
    json[0].HostConfig.CapDrop = (container.capDrop || []).map((cap) => `CAP_${cap}`);
    return JSON.stringify(json);
  };
  await assert.doesNotReject(ensurePod("alpha", projectPath(context, "alpha")));
});

test("destroy removes the owned pod, legacy allocation, and owned private network", async () => {
  const fake = new FakeDocker();
  const context = await setup(fake);
  await ensurePod("alpha", projectPath(context, "alpha"));
  await fs.mkdir(context.stateDir, { recursive: true });
  await fs.writeFile(path.join(context.stateDir, "pods.json"), '{"alpha":{"ip":"10.77.1.2"}}\n');

  await destroyPod("alpha");
  assert.equal(fake.containers.has(podName("alpha")), false);
  assert.equal(fake.networks.has(podNetworkName("alpha")), false);
  assert.deepEqual(await podInspect("alpha"), {
    exists: false, running: false, ip: null, isolated: false, legacySecurity: false
  });
  assert.deepEqual(JSON.parse(await fs.readFile(path.join(context.stateDir, "pods.json"), "utf8")), {});
});

test("corrupt legacy allocation state cannot abort authoritative teardown", async () => {
  const fake = new FakeDocker();
  const context = await setup(fake);
  await ensurePod("alpha", projectPath(context, "alpha"));
  await fs.mkdir(context.stateDir, { recursive: true });
  await fs.writeFile(path.join(context.stateDir, "pods.json"), "{corrupt-json");

  await destroyPod("alpha");
  assert.equal(fake.containers.has(podName("alpha")), false);
  assert.equal(fake.networks.has(podNetworkName("alpha")), false);
  const stopIndex = fake.calls.findIndex(({ args }) => args[0] === "stop");
  const removeContainerIndex = fake.calls.findIndex(({ args }) => args[0] === "rm");
  const removeNetworkIndex = fake.calls.findIndex(({ args }) => args[0] === "network" && args[1] === "rm");
  assert.ok(stopIndex >= 0 && removeContainerIndex > stopIndex && removeNetworkIndex > removeContainerIndex);
});

test("destroy rejects a foreign container name collision without stopping it", async () => {
  const fake = new FakeDocker();
  fake.add("alpha", { running: true });
  fake.containers.get(podName("alpha")).project = "intruder";
  await setup(fake);

  await assert.rejects(destroyPod("alpha"), /pod name collision/);
  assert.equal(fake.containers.get(podName("alpha"))?.running, true);
  assert.equal(fake.calls.some(({ args }) => args[0] === "stop" || args[0] === "rm"), false);
});

test("destroy never deletes a colliding foreign private network", async () => {
  const fake = new FakeDocker();
  fake.addNetwork("alpha", { project: "intruder" });
  await setup(fake);
  await assert.rejects(destroyPod("alpha"), /labelled bridge/);
  assert.equal(fake.networks.has(podNetworkName("alpha")), true);
  assert.equal(fake.calls.some(({ args }) => args[0] === "network" && args[1] === "rm"), false);
});

test("concurrent ensure creates once and queued destroy remains serialized", async () => {
  const fake = new FakeDocker();
  const releaseRun = fake.delayNextRun();
  const context = await setup(fake);
  const first = ensurePod("alpha", projectPath(context, "alpha"));
  const duplicate = ensurePod("alpha", projectPath(context, "alpha"));
  while (!fake.calls.some(({ args }) => args[0] === "run")) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  const destroy = destroyPod("alpha");
  const later = ensurePod("alpha", projectPath(context, "alpha"));
  releaseRun();
  await Promise.all([first, duplicate, destroy, later]);
  assert.equal(fake.calls.filter(({ args }) => args[0] === "run").length, 2);
  assert.equal(fake.containers.get(podName("alpha"))?.running, true);
});

test("project traversal and mismatched backend paths are rejected before Docker", async () => {
  const context = await setup();
  await assert.rejects(ensurePod("../escape", path.join(context.projectsRoot, "escape")), /invalid project name/);
  await assert.rejects(ensurePod("alpha", path.join(context.projectsRoot, "alpha", "child")), /direct directory/);
  await assert.rejects(ensurePod("alpha", path.join(context.projectsRoot, "beta")), /direct directory/);
  assert.equal(context.fake.calls.length, 0);
});

test("podExec preserves argument and host-command deadline behavior", async () => {
  const context = await setup();
  const result = await podExec("alpha", ["sh", "-c", "cat"], { input: "hello" });
  assert.deepEqual(result, { code: 7, stdout: "captured stdout", stderr: "captured stderr" });
  const call = context.fake.calls.at(-1);
  assert.deepEqual(call.args, ["exec", "-i", podName("alpha"), "sh", "-c", "cat"]);
  assert.equal(call.options.input, "hello");
  assert.equal(call.options.timeout, 30_000);
  await podExec("alpha", ["true"], { timeoutMs: 1_234 });
  assert.equal(context.fake.calls.at(-1).options.timeout, 1_234);
  await assert.rejects(podExec("alpha", ["true"], { timeoutMs: 99 }), /timeoutMs/);
});

test("Docker command admission caps active work and rejects queue overflow", async () => {
  const fake = new FakeDocker();
  await setup(fake);
  const release = fake.delayCommands();
  const operations = Array.from({ length: 41 }, (_, index) => podInspect(`queued-${index}`));
  const overflowRejected = assert.rejects(operations[40], /Docker command capacity is busy/);
  try {
    for (let attempt = 0; attempt < 100 && fake.calls.length < 8; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    assert.equal(fake.calls.length, 8);
    await overflowRejected;
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(fake.calls.length, 8);
  } finally {
    release();
  }
  const settled = await Promise.allSettled(operations.slice(0, 40));
  assert.equal(settled.filter(({ status }) => status === "fulfilled").length, 40);
});

