import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { CodexAdapter } from '../../src/agent/codex/adapter.js';
import type { AgentEvent } from '../../src/agent/types.js';

describe('Codex app-server runtime', () => {
  const cleanup: string[] = [];

  afterEach(async () => {
    await Promise.all(cleanup.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
  });

  it('starts the daemon once, uses a proxy, and translates a persistent thread turn', async () => {
    const fake = await createFakeAppServer();
    cleanup.push(fake.dir);
    const adapter = new CodexAdapter({
      binary: fake.path,
      profileStateDir: fake.dir,
      runtime: 'app-server',
      sandbox: 'read-only',
    });

    const first = adapter.run({ runId: 'message-1', prompt: 'first prompt', cwd: fake.dir });
    expect(await collect(first.events)).toEqual([
      { type: 'system', threadId: 'thread-1', cwd: fake.dir },
      { type: 'final_text', content: 'hello from app-server' },
      { type: 'done', threadId: 'thread-1', terminationReason: 'normal' },
    ]);

    const second = adapter.run({
      runId: 'message-2',
      prompt: 'second prompt',
      cwd: fake.dir,
      threadId: 'thread-1',
    });
    expect((await collect(second.events)).at(-1)).toEqual({
      type: 'done',
      threadId: 'thread-1',
      terminationReason: 'normal',
    });

    expect(JSON.parse(await readFile(fake.recordPath, 'utf8'))).toEqual([
      ['app-server', 'daemon', 'start'],
      ['app-server', 'proxy'],
    ]);
    const requests = JSON.parse(await readFile(fake.requestsPath, 'utf8')) as Array<{
      method: string;
      params: Record<string, unknown>;
    }>;
    expect(requests.find((request) => request.method === 'thread/start')?.params).toMatchObject({
      developerInstructions: expect.stringContaining('lark-channel-bridge 运行约定'),
    });
    expect(requests.filter((request) => request.method === 'turn/start')[0]?.params.input).toEqual([
      { type: 'text', text: 'first prompt' },
    ]);
  });
});

async function collect(events: AsyncIterable<AgentEvent>): Promise<AgentEvent[]> {
  const out: AgentEvent[] = [];
  for await (const event of events) out.push(event);
  return out;
}

async function createFakeAppServer(): Promise<{
  path: string;
  dir: string;
  recordPath: string;
  requestsPath: string;
}> {
  const dir = await mkdtemp(join(tmpdir(), 'codex-app-server-test-'));
  const path = join(dir, 'fake-codex.mjs');
  const recordPath = join(dir, 'commands.json');
  const requestsPath = join(dir, 'requests.json');
  await writeFile(
    path,
    [
      '#!/usr/bin/env node',
      'import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";',
      `const recordPath = ${JSON.stringify(recordPath)};`,
      `const requestsPath = ${JSON.stringify(requestsPath)};`,
      'const args = process.argv.slice(2);',
      'const prior = existsSync(recordPath) ? JSON.parse(readFileSync(recordPath, "utf8")) : [];',
      'prior.push(args); writeFileSync(recordPath, JSON.stringify(prior));',
      'if (args.join(" ") === "app-server daemon start") process.exit(0);',
      'if (args.join(" ") !== "app-server proxy") process.exit(2);',
      'let buffer = "";',
      'process.stdin.setEncoding("utf8");',
      'process.stdin.on("data", (chunk) => {',
      '  buffer += chunk;',
      '  while (buffer.includes("\\n")) {',
      '    const index = buffer.indexOf("\\n"); const line = buffer.slice(0, index); buffer = buffer.slice(index + 1);',
      '    if (!line.trim()) continue;',
      '    const request = JSON.parse(line);',
      '    const requests = existsSync(requestsPath) ? JSON.parse(readFileSync(requestsPath, "utf8")) : [];',
      '    requests.push({ method: request.method, params: request.params }); writeFileSync(requestsPath, JSON.stringify(requests));',
      '    if (request.method === "initialize") console.log(JSON.stringify({ id: request.id, result: {} }));',
      '    else if (request.method === "thread/start") console.log(JSON.stringify({ id: request.id, result: { thread: { id: "thread-1" } } }));',
      '    else if (request.method === "thread/resume") console.log(JSON.stringify({ id: request.id, result: { thread: { id: request.params.threadId } } }));',
      '    else if (request.method === "turn/start") {',
      '      console.log(JSON.stringify({ id: request.id, result: { turn: { id: "turn-1" } } }));',
      '      setTimeout(() => {',
      '        console.log(JSON.stringify({ method: "item/agentMessage/delta", params: { threadId: request.params.threadId, turnId: "turn-1", itemId: "item-1", delta: "hello from app-server" } }));',
      '        console.log(JSON.stringify({ method: "item/completed", params: { threadId: request.params.threadId, item: { id: "item-1", type: "agentMessage", text: "hello from app-server" } } }));',
      '        console.log(JSON.stringify({ method: "turn/completed", params: { threadId: request.params.threadId, turn: { id: "turn-1", status: "completed" } } }));',
      '      }, 10);',
      '    } else if (request.method === "turn/interrupt") {',
      '      console.log(JSON.stringify({ id: request.id, result: {} }));',
      '      console.log(JSON.stringify({ method: "turn/completed", params: { threadId: request.params.threadId, turn: { id: request.params.turnId } } }));',
      '    }',
      '  }',
      '});',
    ].join('\n'),
    'utf8',
  );
  await chmod(path, 0o755);
  return { path, dir, recordPath, requestsPath };
}
