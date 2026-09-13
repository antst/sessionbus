// SPDX-License-Identifier: GPL-3.0-only
"use strict";

const { execFileSync } = require('node:child_process');
const { mkdtempSync, mkdirSync, copyFileSync, rmSync } = require('node:fs');
const { tmpdir } = require('node:os');
const { join, resolve } = require('node:path');

const scratch = mkdtempSync(join(tmpdir(), 'sessionbus-types-'));
try {
  const bus = resolve(__dirname, '../..');
  const [pack] = JSON.parse(execFileSync('npm', ['pack', '--json', '--ignore-scripts', '--pack-destination', scratch], { cwd: bus, encoding: 'utf8' }));
  const installed = join(scratch, 'node_modules/@sessionbus/kit');
  mkdirSync(installed, { recursive: true });
  execFileSync('tar', ['-xzf', join(scratch, pack.filename), '--strip-components=1', '-C', installed]);
  copyFileSync(join(__dirname, 'consumer.ts'), join(scratch, 'consumer.ts'));
  execFileSync('npm', ['exec', '--yes', '--package=typescript@5.9.3', '--', 'tsc', '--strict', '--noEmit', '--allowJs', '--maxNodeModuleJsDepth', '1', '--target', 'ES2022', '--module', 'Node16', '--moduleResolution', 'Node16', join(scratch, 'consumer.ts')], { stdio: 'inherit' });
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
