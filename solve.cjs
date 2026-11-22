// solve.cjs — v8 skeleton driver (written from scratch in this session).
// Reads cases.json straight into WASM memory in 512 KiB chunks, runs the
// transducer in logical windows, flushes committed rows per window, and
// assembles answers.json. On transducer decline / tripwire it falls back to
// the kit reference (fallback.js, the only copied code) via JSON.parse.
// main.ts is a one-line require of this file; the driver runs on load.
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { setFlagsFromString } = require('node:v8');

setFlagsFromString('--no-incremental-marking');
if (process.env.MAKEFUN_V8_LIFTOFF_ONLY === '1') {
  setFlagsFromString('--liftoff-only');
}

const INPUT_DIR = process.env.MAKEFUN_INPUT_DIR ?? '/input';
const OUTPUT_DIR = process.env.MAKEFUN_OUTPUT_DIR ?? '/output';
const WIN = Number(process.env.MAKEFUN_V8_WINDOW || 512 * 1024);

const inPath = path.join(INPUT_DIR, 'cases.json');
const outPath = path.join(OUTPUT_DIR, 'answers.json');

const wasmBuf = fs.readFileSync(path.join(__dirname, 'parser.wasm'));
const mod = new WebAssembly.Module(wasmBuf);
const inst = new WebAssembly.Instance(mod, {});
const ex = inst.exports;
const mem = ex.memory;

function view() {
  return new Uint8Array(mem.buffer);
}

function run(input) {
  // JS fallback path (also the kit-compatible run() export).
  const { readCsv } = require('./fallback.js');
  return readCsv(input);
}

function writeFallback(outFd) {
  const raw = fs.readFileSync(inPath);
  const cases = JSON.parse(raw.toString('utf8').replace(/^\uFEFF/, ''));
  const answers = [];
  for (const c of cases) answers.push(run(c.input));
  fs.writeSync(outFd, JSON.stringify(answers));
}

function main() {
  let outFd = fs.openSync(outPath, 'w');
  try {
    let inFd = -1;
    try {
      inFd = fs.openSync(inPath, 'r');
      const size = fs.fstatSync(inFd).size;
      const inPtr = ex.reserve_input(size);
      // read straight into WASM memory, 512 KiB chunks
      let off = 0;
      const CH = 512 * 1024;
      while (off < size) {
        const n = Math.min(CH, size - off);
        const v = new Uint8Array(mem.buffer, inPtr + off, n);
        let got = 0;
        while (got < n) {
          const r = fs.readSync(inFd, v, got, n - got, off + got);
          if (r === 0) throw new Error('short read');
          got += r;
        }
        off += n;
      }
      fs.writeSync(outFd, '[');
      let wroteCase = false;
      let pendingComma = false;
      const flushRows = (n) => {
        if (n <= 0) return;
        if (pendingComma) {
          fs.writeSync(outFd, ',');
          pendingComma = false;
        }
        fs.writeSync(outFd, view().subarray(ex.rows_ptr(), ex.rows_ptr() + n));
        ex.compact_rows(n);
      };
      let winEnd = Math.min(WIN, size);
      let done = false;
      let declined = false;
      const ctrNames = ['esc_2byte', 'esc_slash', 'esc_u_print', 'esc_u_short', 'esc_u_ctrl', 'esc_u_bmp', 'esc_u_pair', 'esc_u_lone', 'esc_u_token'];
      const counters = [];
      for (;;) {
        const isLast = winEnd >= size ? 1 : 0;
        const st = ex.run_window(winEnd, isLast);
        // flush committed rows (a pending ',' goes before the next case)
        flushRows(ex.rows_commit());
        // collect errors into the per-case JS buffer
        const elen = ex.errs_len();
        let errs = null;
        if (elen > 0) {
          errs = Buffer.from(view().subarray(ex.errs_ptr(), ex.errs_ptr() + elen));
          ex.compact_errs(elen);
        }
        if (typeof main._errBuf === 'undefined') main._errBuf = [];
        if (errs) main._errBuf.push(errs);
        if (st === -1 || st === -2) {
          declined = true;
          break;
        }
        if (st === 1) {
          // one case finished: flush its remaining rows, then framing
          flushRows(ex.rows_commit());
          const elen2 = ex.errs_len();
          if (elen2 > 0) {
            main._errBuf.push(Buffer.from(view().subarray(ex.errs_ptr(), ex.errs_ptr() + elen2)));
            ex.compact_errs(elen2);
          }
          const snap = {};
          for (let i = 0; i < ctrNames.length; i++) snap[ctrNames[i]] = ex.counter(i);
          snap.declinedFiles = ex.declined();
          counters.push(snap);
          // rows for this case were already streamed; now close it
          fs.writeSync(outFd, '],"errors":[');
          for (const b of main._errBuf) fs.writeSync(outFd, b);
          main._errBuf = [];
          fs.writeSync(outFd, ']}');
          wroteCase = true;
          pendingComma = true; // ',' goes before the next case's rows
          continue; // same window: next case may start here
        }
        if (st === 2) {
          done = true;
          break;
        }
        // st === 0: paused at window end
        if (winEnd >= size) throw new Error('transducer paused at EOF');
        winEnd = Math.min(size, winEnd + WIN);
      }
      if (declined) {
        fs.closeSync(outFd);
        outFd = fs.openSync(outPath, 'w');
        writeFallback(outFd);
      } else if (done) {
        fs.writeSync(outFd, wroteCase ? ']' : ']');
      }
      if (process.env.MAKEFUN_V8_COUNTERS === '1') {
        fs.writeFileSync(
          path.join(OUTPUT_DIR, 'v8-counters.json'),
          JSON.stringify({ cases: ex.cases_done(), tripped: ex.tripped(), declined: ex.declined(), counters }, null, 2)
        );
      }
    } finally {
      if (inFd >= 0) fs.closeSync(inFd);
    }
  } finally {
    fs.closeSync(outFd);
  }
}

main();

module.exports = { run };
