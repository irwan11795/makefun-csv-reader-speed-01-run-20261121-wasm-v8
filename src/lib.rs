// csv8 — skeleton v8: single-pass streaming JSON-to-JSON CSV transducer.
// Written from scratch in this session (skeleton experiment, csv-reader-speed-01).
// Semantics follow the kit reference (csv-reader 1.0.0 readCsv) exactly; the
// only code copied from the kit is fallback.js (the reference itself).
//
// Architecture: the driver reads cases.json straight into WASM linear memory
// in 512 KiB readSync chunks; the parser runs in logical windows (run_window)
// and pauses at window ends whenever a decision needs a byte from the next
// window (split escape, trailing \r, split surrogate pair, split UTF-8).
// Input positions are absolute file offsets, so no carry memmove is needed
// for correctness; output rows/errors are flushed per window up to the
// commit point only. Whole-file input residency (like v6p) is a deliberate
// skeleton simplification, noted in the build report.

#![allow(static_mut_refs)]

use core::arch::wasm32::*;
use core::ptr;

// ---------- return codes from run_window ----------
const RC_PAUSE: i32 = 0; // need next window
const RC_CASE: i32 = 1; // one case finished (driver finalises framing)
const RC_FILE: i32 = 2; // whole file finished
const RC_DECLINE: i32 = -1; // malformed / unsupported envelope -> fallback
const RC_TRIP: i32 = -2; // output bound tripped (plan bug, not a bail)

// ---------- phases ----------
const PH_ENV0: u8 = 0; // expect '[' (or BOM + '[')
const PH_ENV_CASE: u8 = 1; // expect {"id":"..." or ']'
const PH_SPAN_SCAN: u8 = 2; // find raw '"' ending the text span
const PH_HEADER: u8 = 3; // expect ,"header":true/false}}
const PH_CSV: u8 = 4; // parse the case text
const PH_FILE_END: u8 = 5; // trailing whitespace / EOF

// ---------- csv sub-states ----------
const ST_REC: u8 = 0; // record start: skip empty lines
const ST_FIELD: u8 = 1; // field start: peek quote?
const ST_UNQ: u8 = 2; // unquoted field
const ST_QUO: u8 = 3; // quoted field
const ST_AFTER: u8 = 4; // just after closing quote
const ST_JUNK: u8 = 5; // junk after closing quote

// ---------- field/record end ----------
const END_COMMA: u8 = 0;
const END_BREAK: u8 = 1;
const END_TEXT: u8 = 2;

const ROWS_CAP: usize = 4 * 1024 * 1024;
const ERRS_CAP: usize = 512 * 1024;

// Error / pause signalling inside the parser.
const E_PAUSE: u8 = 1;
const E_DECLINE: u8 = 2;
const E_TRIP: u8 = 3;

struct State {
    in_ptr: *mut u8,
    in_len: u32,
    rows_ptr: *mut u8,
    errs_ptr: *mut u8,
    wr: u32,
    we: u32,
    pos: u32,      // absolute input offset (envelope cursor / csv cursor)
    win_end: u32,  // end of current logical window
    is_last: bool, // win_end == in_len
    phase: u8,
    sub: u8, // envelope sub-state (resume-safe literal matching)
    exp_buf: [u8; 24],
    exp_len: u8,
    exp_idx: u8,
    // envelope
    scan_pos: u32, // span-scan cursor
    span_start: u32,
    span_end: u32,
    env_pos: u32, // envelope cursor after the case's closing }}
    file_end_after_case: bool,
    cases_done: u32,
    declined_files: u32,
    tripped: u32,
    // csv per-case
    header: bool,
    line: u32,
    record_line: u32,
    record_row: i32, // -1 = null (header record)
    width: i32,      // -1 unset
    field_idx: i32,
    rows_count: u32,
    rows_started: bool,
    columns_done: bool,
    st: u8,
    field_quoted: bool,
    field_start_out: u32,
    field_content_start: u32,
    junk_start_out: u32,
    row_start_out: u32,
    row_commit_out: u32,
    commit_out: u32,
    long_pending: bool,
    open: u32,
    open_line: u32,
    open_len: u32,
    errs_count: u32,
    // counters (§6a), per case
    c_2byte: u32,
    c_slash: u32,
    c_u_print: u32,
    c_u_short: u32,
    c_u_ctrl: u32,
    c_u_bmp: u32,
    c_u_pair: u32,
    c_u_lone: u32,
    c_u_token: u32,
}

impl State {
    const fn new() -> State {
        State {
            in_ptr: ptr::null_mut(),
            in_len: 0,
            rows_ptr: ptr::null_mut(),
            errs_ptr: ptr::null_mut(),
            wr: 0,
            we: 0,
            pos: 0,
            win_end: 0,
            is_last: false,
            phase: PH_ENV0,
            sub: 0,
            exp_buf: [0u8; 24],
            exp_len: 0,
            exp_idx: 0,
            scan_pos: 0,
            span_start: 0,
            span_end: 0,
            env_pos: 0,
            file_end_after_case: false,
            cases_done: 0,
            declined_files: 0,
            tripped: 0,
            header: false,
            line: 1,
            record_line: 1,
            record_row: -1,
            width: -1,
            field_idx: 0,
            rows_count: 0,
            rows_started: false,
            columns_done: false,
            st: ST_REC,
            field_quoted: false,
            field_start_out: 0,
            field_content_start: 0,
            junk_start_out: 0,
            row_start_out: 0,
            row_commit_out: 0,
            commit_out: 0,
            long_pending: false,
            open: 0,
            open_line: 1,
            open_len: 0,
            errs_count: 0,
            c_2byte: 0,
            c_slash: 0,
            c_u_print: 0,
            c_u_short: 0,
            c_u_ctrl: 0,
            c_u_bmp: 0,
            c_u_pair: 0,
            c_u_lone: 0,
            c_u_token: 0,
        }
    }
}

static mut S: State = State::new();
static mut ERR: u8 = 0; // 0 ok, else E_*

// ---------- low-level helpers (unsafe, hot) ----------
#[inline(always)]
unsafe fn ib(p: u32) -> u8 {
    *S.in_ptr.add(p as usize)
}
#[inline(always)]
unsafe fn out_b(b: u8) {
    // caller guarantees capacity (checked at bulk points + per-field)
    *S.rows_ptr.add(S.wr as usize) = b;
    S.wr += 1;
}
#[inline(always)]
unsafe fn out_slice(src: *const u8, n: usize) {
    ptr::copy_nonoverlapping(src, S.rows_ptr.add(S.wr as usize), n);
    S.wr += n as u32;
}
#[inline(always)]
unsafe fn out_str(s: &[u8]) {
    out_slice(s.as_ptr(), s.len());
}
#[inline(always)]
unsafe fn err_b(b: u8) {
    *S.errs_ptr.add(S.we as usize) = b;
    S.we += 1;
}
#[inline(always)]
unsafe fn err_str(s: &[u8]) {
    ptr::copy_nonoverlapping(s.as_ptr(), S.errs_ptr.add(S.we as usize), s.len());
    S.we += s.len() as u32;
}
#[inline(always)]
unsafe fn rows_room(n: u32) -> bool {
    (S.wr as usize) + (n as usize) <= ROWS_CAP
}
#[inline(always)]
unsafe fn errs_room(n: u32) -> bool {
    (S.we as usize) + (n as usize) <= ERRS_CAP
}

// byte at absolute offset q: Ok(Some(b)) if available, Ok(None) if past
// window-end AND this is the last window (genuinely absent), Err(Pause)
// if past window-end but more input is coming.
#[inline(always)]
unsafe fn avail(q: u32) -> Result<Option<u8>, u8> {
    if q < S.win_end {
        Ok(Some(ib(q)))
    } else if S.is_last {
        Ok(None)
    } else {
        Err(E_PAUSE)
    }
}

// manual itoa into errs
unsafe fn err_u32(mut v: u32) {
    let mut buf = [0u8; 10];
    let mut n = 0;
    if v == 0 {
        err_b(b'0');
        return;
    }
    while v > 0 {
        buf[n] = b'0' + (v % 10) as u8;
        v /= 10;
        n += 1;
    }
    while n > 0 {
        n -= 1;
        err_b(buf[n]);
    }
}
unsafe fn err_i32_or_null(v: i32) {
    if v < 0 {
        err_str(b"null");
    } else {
        err_u32(v as u32);
    }
}

fn hex_val(b: u8) -> Option<u32> {
    match b {
        b'0'..=b'9' => Some((b - b'0') as u32),
        b'a'..=b'f' => Some((b - b'a' + 10) as u32),
        b'A'..=b'F' => Some((b - b'A' + 10) as u32),
        _ => None,
    }
}
fn hex_lo(n: u32) -> u8 {
    let d = (n & 0xF) as u8;
    if d < 10 {
        b'0' + d
    } else {
        b'a' + d - 10
    }
}

// ---------- error objects ----------
unsafe fn push_err(code: &[u8]) {
    // {"code":"X","line":N,"row":R|null}
    if !errs_room(64) {
        ERR = E_TRIP;
        return;
    }
    if S.errs_count > 0 {
        err_b(b',');
    }
    err_str(b"{\"code\":\"");
    err_str(code);
    err_str(b"\",\"line\":");
    err_u32(S.record_line);
    err_str(b",\"row\":");
    err_i32_or_null(S.record_row);
    err_b(b'}');
    S.errs_count += 1;
}

// ---------- UTF-8 ----------
unsafe fn utf8_len_lead(b: u8) -> Option<usize> {
    match b {
        0xC2..=0xDF => Some(2),
        0xE0..=0xEF => Some(3),
        0xF0..=0xF4 => Some(4),
        _ => None,
    }
}
// Validate + copy one raw UTF-8 sequence starting at S.pos (pos < span_end,
// byte >= 0x80). Returns Ok(len). Declines on invalid UTF-8.
unsafe fn copy_utf8_seq() -> Result<usize, u8> {
    let p = S.pos;
    let b0 = ib(p);
    let len = match utf8_len_lead(b0) {
        Some(l) => l,
        None => return Err(E_DECLINE),
    };
    // continuation bytes, with window pause
    for k in 1..len {
        match avail(p + k as u32)? {
            Some(c) if (0x80..=0xBF).contains(&c) => {}
            Some(_) => return Err(E_DECLINE),
            None => return Err(E_DECLINE), // past span/file end: truncated
        }
    }
    let b1 = ib(p + 1);
    match len {
        3 => {
            if b0 == 0xE0 && b1 < 0xA0 {
                return Err(E_DECLINE);
            }
            if b0 == 0xED && b1 > 0x9F {
                return Err(E_DECLINE);
            }
        }
        4 => {
            if b0 == 0xF0 && b1 < 0x90 {
                return Err(E_DECLINE);
            }
            if b0 == 0xF4 && b1 > 0x8F {
                return Err(E_DECLINE);
            }
        }
        _ => {}
    }
    // BOM drop: raw EF BB BF at the very start of the text
    if len == 3 && p == S.span_start && b0 == 0xEF && ib(p + 1) == 0xBB && ib(p + 2) == 0xBF {
        return Ok(len); // caller advances pos but emits nothing
    }
    if !rows_room(len as u32) {
        return Err(E_TRIP);
    }
    out_slice(S.in_ptr.add(p as usize), len);
    Ok(len)
}

// encode code point as UTF-8 into rows (cp < 0xD800 or cp > 0xDFFF, cp != 0)
unsafe fn emit_utf8_cp(cp: u32) {
    if cp < 0x80 {
        out_b(cp as u8);
    } else if cp < 0x800 {
        out_b((0xC0 | (cp >> 6)) as u8);
        out_b((0x80 | (cp & 0x3F)) as u8);
    } else if cp < 0x10000 {
        out_b((0xE0 | (cp >> 12)) as u8);
        out_b((0x80 | ((cp >> 6) & 0x3F)) as u8);
        out_b((0x80 | (cp & 0x3F)) as u8);
    } else {
        out_b((0xF0 | (cp >> 18)) as u8);
        out_b((0x80 | ((cp >> 12) & 0x3F)) as u8);
        out_b((0x80 | ((cp >> 6) & 0x3F)) as u8);
        out_b((0x80 | (cp & 0x3F)) as u8);
    }
}
unsafe fn emit_lone_surrogate(cp: u32) {
    // \udXXX lowercase, 6 bytes
    out_str(b"\\u");
    out_b(hex_lo(cp >> 12));
    out_b(hex_lo(cp >> 8));
    out_b(hex_lo(cp >> 4));
    out_b(hex_lo(cp));
}

// ---------- tokens ----------
// A token is one decoded CSV unit starting at pos: quote / comma / LF / CR /
// literal. peek_quote(pos) -> Ok((is_quote, token_len)).
unsafe fn peek_quote(q: u32) -> Result<(bool, u32), u8> {
    match avail(q)? {
        Some(b'\\') => match avail(q + 1)? {
            Some(b'"') => Ok((true, 2)),
            Some(b'u') => match read_u(q + 2)? {
                Some(cp) => Ok((cp == 0x22, 6)),
                None => Ok((false, 0)),
            },
            _ => Ok((false, 0)),
        },
        _ => Ok((false, 0)),
    }
}
// read \uXXXX at q (q points at the 'u'... actually q points just after "\u",
// i.e. first hex digit). Returns Ok(Some(cp)) / Ok(None) if not \u (caller
// checked), Err on malformed / pause.
unsafe fn read_u(q: u32) -> Result<Option<u32>, u8> {
    let mut cp: u32 = 0;
    for k in 0..4 {
        match avail(q + k)? {
            Some(c) => match hex_val(c) {
                Some(v) => cp = (cp << 4) | v,
                None => return Err(E_DECLINE),
            },
            None => return Err(E_DECLINE),
        }
    }
    Ok(Some(cp))
}

// Classify the escape starting at pos (ib(pos) == '\\').
// Returns the decoded class; consumes via S.pos. Used by the field scanners.
// Token kinds returned in `kind`: 0 literal-emitted, 1 quote, 2 comma,
// 3 LF, 4 CR. For kind 0 the bytes were already emitted.
unsafe fn escape_step() -> Result<u8, u8> {
    let p = S.pos;
    let b2 = match avail(p + 1)? {
        Some(b) => b,
        None => return Err(E_DECLINE), // dangling backslash at end of text
    };
    match b2 {
        b'"' => {
            S.pos += 2;
            S.c_2byte += 1;
            Ok(1)
        }
        b'\\' => {
            if !rows_room(2) {
                return Err(E_TRIP);
            }
            out_str(b"\\\\");
            S.pos += 2;
            S.c_2byte += 1;
            Ok(0)
        }
        b'/' => {
            if !rows_room(1) {
                return Err(E_TRIP);
            }
            out_b(b'/');
            S.pos += 2;
            S.c_slash += 1;
            Ok(0)
        }
        b'b' => {
            if !rows_room(2) {
                return Err(E_TRIP);
            }
            out_str(b"\\b");
            S.pos += 2;
            S.c_2byte += 1;
            Ok(0)
        }
        b'f' => {
            if !rows_room(2) {
                return Err(E_TRIP);
            }
            out_str(b"\\f");
            S.pos += 2;
            S.c_2byte += 1;
            Ok(0)
        }
        b'n' => {
            S.pos += 2;
            Ok(3)
        }
        b'r' => {
            S.pos += 2;
            Ok(4)
        }
        b't' => {
            if !rows_room(2) {
                return Err(E_TRIP);
            }
            out_str(b"\\t");
            S.pos += 2;
            S.c_2byte += 1;
            Ok(0)
        }
        b'u' => escape_u(),
        _ => Err(E_DECLINE),
    }
}

// \uXXXX escape at S.pos (points at '\'). Inline decode per §2b.
unsafe fn escape_u() -> Result<u8, u8> {
    let p = S.pos;
    let cp = match read_u(p + 2)? {
        Some(c) => c,
        None => return Err(E_DECLINE),
    };
    match cp {
        0x22 => {
            S.pos += 6;
            S.c_u_token += 1;
            Ok(1)
        }
        0x2C => {
            S.pos += 6;
            S.c_u_token += 1;
            Ok(2)
        }
        0x0A => {
            S.pos += 6;
            S.c_u_token += 1;
            Ok(3)
        }
        0x0D => {
            S.pos += 6;
            S.c_u_token += 1;
            Ok(4)
        }
        0x5C => {
            if !rows_room(2) {
                return Err(E_TRIP);
            }
            out_str(b"\\\\");
            S.pos += 6;
            S.c_u_token += 1;
            Ok(0)
        }
        0x08 => {
            if !rows_room(2) {
                return Err(E_TRIP);
            }
            out_str(b"\\b");
            S.pos += 6;
            S.c_u_short += 1;
            Ok(0)
        }
        0x09 => {
            if !rows_room(2) {
                return Err(E_TRIP);
            }
            out_str(b"\\t");
            S.pos += 6;
            S.c_u_short += 1;
            Ok(0)
        }
        0x0C => {
            if !rows_room(2) {
                return Err(E_TRIP);
            }
            out_str(b"\\f");
            S.pos += 6;
            S.c_u_short += 1;
            Ok(0)
        }
        0x00..=0x1F => {
            if !rows_room(6) {
                return Err(E_TRIP);
            }
            out_str(b"\\u00");
            out_b(hex_lo(cp >> 4));
            out_b(hex_lo(cp));
            S.pos += 6;
            S.c_u_ctrl += 1;
            Ok(0)
        }
        0x20..=0x7E => {
            if !rows_room(1) {
                return Err(E_TRIP);
            }
            out_b(cp as u8);
            S.pos += 6;
            S.c_u_print += 1;
            Ok(0)
        }
        0x7F => {
            if !rows_room(1) {
                return Err(E_TRIP);
            }
            out_b(0x7F);
            S.pos += 6;
            S.c_u_print += 1;
            Ok(0)
        }
        0xD800..=0xDBFF => {
            // high surrogate: peek next \uDC00-\uDFFF
            let pair = peek_surrogate_pair(p + 6)?;
            if let Some(lo) = pair {
                if !rows_room(4) {
                    return Err(E_TRIP);
                }
                let cpoint = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                emit_utf8_cp(cpoint);
                S.pos += 12;
                S.c_u_pair += 1;
            } else {
                if !rows_room(6) {
                    return Err(E_TRIP);
                }
                emit_lone_surrogate(cp);
                S.pos += 6;
                S.c_u_lone += 1;
            }
            Ok(0)
        }
        0xDC00..=0xDFFF => {
            if !rows_room(6) {
                return Err(E_TRIP);
            }
            emit_lone_surrogate(cp);
            S.pos += 6;
            S.c_u_lone += 1;
            Ok(0)
        }
        _ => {
            // BMP non-ASCII (incl. U+FEFF, U+2028/2029)
            if cp == 0xFEFF && p == S.span_start {
                S.pos += 6; // BOM drop
                S.c_u_bmp += 1;
                return Ok(0);
            }
            let n = if cp < 0x800 { 2 } else { 3 };
            if !rows_room(n) {
                return Err(E_TRIP);
            }
            emit_utf8_cp(cp);
            S.pos += 6;
            S.c_u_bmp += 1;
            Ok(0)
        }
    }
}

// At absolute q: is there a \uDC00-\uDFFF escape? Ok(Some(lo)) / Ok(None).
unsafe fn peek_surrogate_pair(q: u32) -> Result<Option<u32>, u8> {
    match avail(q)? {
        Some(b'\\') => match avail(q + 1)? {
            Some(b'u') => match read_u(q + 2)? {
                Some(cp) if (0xDC00..=0xDFFF).contains(&cp) => Ok(Some(cp)),
                Some(_) => Ok(None),
                None => Ok(None),
            },
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

// Count a quote token consumed via peek (ST_FIELD open / doubled second).
unsafe fn count_quote_token(qlen: u32) {
    unsafe {
        if qlen == 6 {
            S.c_u_token += 1;
        } else {
            S.c_2byte += 1;
        }
    }
}

// Is the token at q an LF token? (\\n or \\u000A)
unsafe fn peek_lf(q: u32) -> Result<bool, u8> {
    match avail(q)? {
        Some(b'\\') => match avail(q + 1)? {
            Some(b'n') => Ok(true),
            Some(b'u') => match read_u(q + 2)? {
                Some(0x0A) => Ok(true),
                _ => Ok(false),
            },
            _ => Ok(false),
        },
        _ => Ok(false),
    }
}

// ---------- v128 clean-run scan ----------
// Scan from S.pos up to `lim` (<= win_end, <= span_end). Copies clean runs
// verbatim to rows. Returns Ok(true) if a "hit" byte stops the run at S.pos
// (caller scalar-steps it), Ok(false) if pos reached lim.
// `comma` = whether raw ',' counts as a hit (unquoted/junk), false in quotes.
unsafe fn scan_run(lim: u32, comma: bool) -> Result<bool, u8> {
    while S.pos + 16 <= lim {
        let v = v128_load(S.in_ptr.add(S.pos as usize) as *const v128);
        let mut m = i8x16_bitmask(i8x16_lt(v, i8x16_splat(0x20i8)));
        // delimiters: '\' and '"', plus ',' when comma==true
        let mut d = v128_or(
            i8x16_eq(v, i8x16_splat(b'\\' as i8)),
            i8x16_eq(v, i8x16_splat(b'"' as i8)),
        );
        if comma {
            d = v128_or(d, i8x16_eq(v, i8x16_splat(b',' as i8)));
        }
        m |= i8x16_bitmask(d);
        if m == 0 {
            if !rows_room(16) {
                return Err(E_TRIP);
            }
            out_slice(S.in_ptr.add(S.pos as usize), 16);
            S.pos += 16;
        } else {
            let i = m.trailing_zeros();
            if i > 0 {
                if !rows_room(i) {
                    return Err(E_TRIP);
                }
                out_slice(S.in_ptr.add(S.pos as usize), i as usize);
                S.pos += i;
            }
            return Ok(true);
        }
    }
    Ok(S.pos < lim)
}

// After a CR token (consumed, S.pos points just past it): is the next token
// an LF token? Ok(true) = CRLF, Ok(false) = lone CR. Pauses at window end.
unsafe fn cr_followed_by_lf() -> Result<bool, u8> {
    let q = S.pos;
    if q >= S.span_end {
        return Ok(false);
    }
    if q >= S.win_end && !S.is_last {
        return Err(E_PAUSE);
    }
    peek_lf(q)
}

// Scalar step for one byte at S.pos in unquoted/junk/quoted content.
// Handles ',', '\\' (escape), '"' (span end -> returns END_TEXT via `end`),
// raw control (decline), UTF-8 sequences, plain ASCII.
// Returns Ok(Some(end_kind)) when the field ends, Ok(None) to continue.
// `quoted` changes comma handling (content vs delimiter) and quote handling.
unsafe fn content_step(quoted: bool) -> Result<Option<u8>, u8> {
    let p = S.pos;
    if p >= S.span_end {
        return Ok(Some(END_TEXT));
    }
    if p >= S.win_end {
        if S.is_last {
            return Ok(Some(END_TEXT));
        }
        return Err(E_PAUSE);
    }
    let b = ib(p);
    match b {
        b',' => {
            if quoted {
                if !rows_room(1) {
                    return Err(E_TRIP);
                }
                out_b(b',');
                S.pos += 1;
                Ok(None)
            } else {
                S.pos += 1;
                Ok(Some(END_COMMA))
            }
        }
        b'\\' => {
            let k = escape_step()?;
            match k {
                0 => Ok(None), // literal emitted
                1 => {
                    // quote token
                    if quoted {
                        // closing or doubled: peek next token
                        handle_quote_in_quoted()
                    } else {
                        // literal '"' in unquoted content
                        if !rows_room(2) {
                            return Err(E_TRIP);
                        }
                        out_str(b"\\\"");
                        Ok(None)
                    }
                }
                2 => {
                    // comma token
                    if quoted {
                        if !rows_room(1) {
                            return Err(E_TRIP);
                        }
                        out_b(b',');
                        Ok(None)
                    } else {
                        Ok(Some(END_COMMA))
                    }
                }
                3 => Ok(Some(3)),  // LF token: caller handles line++/emit
                4 => Ok(Some(99)), // CR token: caller handles via cr_step
                _ => Ok(None),
            }
        }
        b'"' => Ok(Some(END_TEXT)), // raw quote = span terminator
        0x00..=0x1F => Err(E_DECLINE), // raw control byte: malformed JSON
        0x80..=0xFF => {
            let len = copy_utf8_seq()?;
            // BOM-drop case returns len but emitted nothing
            S.pos += len as u32;
            Ok(None)
        }
        _ => {
            if !rows_room(1) {
                return Err(E_TRIP);
            }
            out_b(b);
            S.pos += 1;
            Ok(None)
        }
    }
}

// quote token consumed at S.pos already points past it (escape_step advanced
// pos). Decide doubled vs closing in S_QUOTED.
unsafe fn handle_quote_in_quoted() -> Result<Option<u8>, u8> {
    let q = S.pos;
    if q >= S.span_end {
        // next token is past the span: closing quote
        S.st = ST_AFTER;
        return Ok(None);
    }
    if q >= S.win_end && !S.is_last {
        // cannot peek the next token: rewind so the quote token is
        // re-processed next window. escape_step already advanced pos past
        // the quote token; step back over it (2 or 6 bytes: recompute).
        // The token length is known from the byte before pos... simpler:
        // caller guarantees this path is only reached when the peek is
        // possible; see scan loop guard below.
        return Err(E_PAUSE);
    }
    match peek_quote(q)? {
        (true, _len) => {
            // doubled quote: consume second quote token, emit \"
            let (_, l2) = peek_quote(q).unwrap();
            count_quote_token(l2);
            if !rows_room(2) {
                return Err(E_TRIP);
            }
            out_str(b"\\\"");
            S.pos = q + l2;
            Ok(None)
        }
        (false, _) => {
            S.st = ST_AFTER;
            Ok(None)
        }
    }
}

// CR token just consumed (kind 4). Resolve CRLF vs lone CR.
// `quoted`: inside quotes -> emit \n + line++; outside -> line break or stray.
// Returns Ok(Some(end)) when the field/record ends, Ok(None) to continue.
unsafe fn cr_step(quoted: bool) -> Result<Option<u8>, u8> {
    if cr_followed_by_lf()? {
        // consume the LF token (2 or 6 bytes)
        let q = S.pos;
        let l = if ib(q) == b'\\' && avail(q + 1)?.unwrap_or(0) == b'n' {
            2
        } else {
            6
        };
        S.pos += l;
        S.line += 1;
        if quoted {
            if !rows_room(2) {
                return Err(E_TRIP);
            }
            out_str(b"\\n");
            Ok(None)
        } else {
            Ok(Some(END_BREAK))
        }
    } else {
        // lone CR: a line break (line++) ONLY inside quotes; outside
        // quotes it is dropped as stray_cr with no line increment
        // (reference readUnquoted: error + pos += 1, line untouched).
        if quoted {
            S.line += 1;
            if !rows_room(2) {
                return Err(E_TRIP);
            }
            out_str(b"\\n");
            Ok(None)
        } else {
            push_err(b"stray_cr");
            if ERR != 0 {
                return Err(ERR);
            }
            Ok(None)
        }
    }
}

// One field scan in state ST_UNQ / ST_JUNK / ST_QUO until the field ends.
// Returns Ok(end_kind). `quoted` = S_QUO, `junk` = S_JUNK.
unsafe fn scan_field(quoted: bool, _junk: bool) -> Result<u8, u8> {
    loop {
        let lim = if S.span_end < S.win_end {
            S.span_end
        } else {
            S.win_end
        };
        if S.pos >= lim {
            if S.pos >= S.span_end {
                return Ok(END_TEXT);
            }
            if S.is_last {
                return Ok(END_TEXT);
            }
            return Err(E_PAUSE);
        }
        // v128 run (stops at first hit byte)
        let hit = scan_run(lim, !quoted)?;
        if !hit {
            // reached lim without a hit
            if S.pos >= S.span_end {
                return Ok(END_TEXT);
            }
            if S.is_last {
                return Ok(END_TEXT);
            }
            return Err(E_PAUSE);
        }
        // scalar-step the hit byte; guard the quoted-quote lookahead: if
        // the hit is a backslash starting a quote token, the doubled/closing
        // decision needs the NEXT token, which may cross win_end. escape_step
        // consumes the quote token first; handle_quote_in_quoted then peeks.
        // To avoid rewinding, pre-check here: if this escape is a quote
        // token, verify the following token is fully inside the window (or
        // past span_end / last window) before consuming it.
        let b = ib(S.pos);
        if quoted && b == b'\\' {
            let (is_q, qlen) = peek_quote(S.pos)?;
            if is_q {
                let after = S.pos + qlen;
                if after < S.span_end && after >= S.win_end && !S.is_last {
                    return Err(E_PAUSE);
                }
                // also the token after that (for doubled check) must be
                // peekable: peek_quote(after) needs up to 6 bytes
                if after < S.span_end && after + 6 > S.win_end && !S.is_last {
                    // peek_quote may still succeed if the token is short
                    // (\" is 2 bytes); only pause when it actually fails.
                    // Try it: if it errors with pause, propagate.
                    match peek_quote(after) {
                        Ok(_) => {}
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        match content_step(quoted)? {
            None => {
                if S.st == ST_AFTER {
                    return Ok(98); // closed quote -> after-quote handling
                }
                continue;
            }
            Some(99) => {
                // CR token
                match cr_step(quoted)? {
                    None => continue,
                    Some(e) => return Ok(e),
                }
            }
            Some(3) => {
                // LF token (consumed by escape_step)
                S.line += 1;
                if quoted {
                    S.c_2byte += 1; // \n as quoted-field content
                    if !rows_room(2) {
                        return Err(E_TRIP);
                    }
                    out_str(b"\\n");
                    continue;
                }
                return Ok(END_BREAK);
            }
            Some(e) => return Ok(e), // END_COMMA / END_TEXT
        }
    }
}

// ---------- token length helpers ----------
unsafe fn lf_len(q: u32) -> Result<Option<u32>, u8> {
    match avail(q)? {
        Some(b'\\') => match avail(q + 1)? {
            Some(b'n') => Ok(Some(2)),
            Some(b'u') => match read_u(q + 2)? {
                Some(0x0A) => Ok(Some(6)),
                _ => Ok(None),
            },
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}
unsafe fn cr_len(q: u32) -> Result<Option<u32>, u8> {
    match avail(q)? {
        Some(b'\\') => match avail(q + 1)? {
            Some(b'r') => Ok(Some(2)),
            Some(b'u') => match read_u(q + 2)? {
                Some(0x0D) => Ok(Some(6)),
                _ => Ok(None),
            },
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}
unsafe fn comma_len(q: u32) -> Result<Option<u32>, u8> {
    match avail(q)? {
        Some(b',') => Ok(Some(1)),
        Some(b'\\') => match avail(q + 1)? {
            Some(b'u') => match read_u(q + 2)? {
                Some(0x2C) => Ok(Some(6)),
                _ => Ok(None),
            },
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

// ---------- field / record helpers ----------
unsafe fn err_sep() {
    if S.errs_count > 0 {
        err_b(b',');
    }
}

// close the field just scanned: write JSON closing quote, update
// row_commit_out / long_pending. Returns field_empty.
unsafe fn close_field() -> Result<bool, u8> {
    let empty = S.wr == S.field_content_start;
    if !rows_room(1) {
        return Err(E_TRIP);
    }
    out_b(b'"');
    if S.width >= 0 && S.field_idx == S.width - 1 {
        S.row_commit_out = S.wr;
    }
    if S.width >= 0 && S.field_idx >= S.width {
        S.long_pending = true;
    }
    Ok(empty)
}

unsafe fn push_short_row(nfields: i32) {
    if !errs_room(80) {
        ERR = E_TRIP;
        return;
    }
    err_sep();
    err_str(b"{\"code\":\"short_row\",\"line\":");
    err_u32(S.record_line);
    err_str(b",\"row\":");
    err_i32_or_null(S.record_row);
    err_str(b",\"fields\":");
    err_u32(nfields as u32);
    err_b(b'}');
    S.errs_count += 1;
}

unsafe fn push_long_row() -> Result<(), u8> {
    let region = S.wr - S.row_commit_out; // starts with ',' separator
    if region < 2 {
        return Err(E_TRIP); // should not happen: long => >= 1 extra field
    }
    let need = 80 + region;
    if !errs_room(need) {
        return Err(E_TRIP);
    }
    err_sep();
    err_str(b"{\"code\":\"long_row\",\"line\":");
    err_u32(S.record_line);
    err_str(b",\"row\":");
    err_i32_or_null(S.record_row);
    err_str(b",\"extra\":[");
    // copy extra fields, skipping the leading ',' separator
    ptr::copy_nonoverlapping(
        S.rows_ptr.add((S.row_commit_out + 1) as usize),
        S.errs_ptr.add(S.we as usize),
        (region - 1) as usize,
    );
    S.we += region - 1;
    err_str(b"]}");
    S.errs_count += 1;
    Ok(())
}

// If the case text ends with no columns written (header, zero records —
// including records that were all discarded as no-record lines), emit the
// empty columns array now.
unsafe fn close_case_text(end: u8) -> Result<bool, u8> {
    if end == END_TEXT && S.header && !S.columns_done {
        if !rows_room(10) {
            return Err(E_TRIP);
        }
        out_str(b"[],\"rows\":[");
        S.commit_out = S.wr;
    }
    Ok(end == END_TEXT)
}

// Finish the record whose last field just closed. `end` = END_BREAK/END_TEXT.
// Returns Ok(true) if the whole case text is done.
unsafe fn finish_record(end: u8, field_empty: bool) -> Result<bool, u8> {
    if S.field_idx == 0 && field_empty && !S.field_quoted {
        // no record (single empty unquoted field); errors stay
        S.wr = S.row_start_out;
        return close_case_text(end);
    }
    let nfields = S.field_idx + 1;
    if S.width < 0 {
        S.width = nfields;
    }
    if S.header && !S.columns_done {
        S.columns_done = true;
        if !rows_room(9) {
            return Err(E_TRIP);
        }
        out_str(b"],\"rows\":[");
        S.commit_out = S.wr;
    } else {
        if nfields < S.width {
            push_short_row(nfields);
            if ERR != 0 {
                return Err(ERR);
            }
            let diff = (S.width - nfields) as u32;
            if !rows_room(3 * diff) {
                return Err(E_TRIP);
            }
            for _ in 0..diff {
                out_str(b",\"\"");
            }
        } else if nfields > S.width {
            push_long_row()?;
            S.wr = S.row_commit_out;
        }
        if !rows_room(1) {
            return Err(E_TRIP);
        }
        out_b(b']');
        S.rows_count += 1;
        S.rows_started = true;
        S.commit_out = S.wr;
    }
    close_case_text(end)
}

unsafe fn end_field_comma() -> Result<(), u8> {
    let _ = close_field()?;
    S.field_idx += 1;
    S.st = ST_FIELD;
    Ok(())
}

unsafe fn end_field_record(end: u8) -> Result<bool, u8> {
    let empty = close_field()?;
    finish_record(end, empty)
}

// ---------- the per-case CSV parser ----------
// Runs until the case text is done (Ok(true)), a pause is needed
// (Err(E_PAUSE)), or decline/trip (Err(E_DECLINE)/Err(E_TRIP)).
unsafe fn parse_case() -> Result<bool, u8> {
    loop {
        match S.st {
            ST_REC => {
                // BOM: drop U+FEFF at the very start of the text, BEFORE
                // field-type detection (reference: text[0] === '\uFEFF'
                // is skipped before any field is read). Without this the
                // first field is misclassified as unquoted whenever the
                // text starts with a BOM.
                if S.pos == S.span_start {
                    match avail(S.pos)? {
                        Some(0xEF) => {
                            let b1 = avail(S.pos + 1)?;
                            let b2 = avail(S.pos + 2)?;
                            if b1 == Some(0xBB) && b2 == Some(0xBF) {
                                S.pos += 3;
                            }
                        }
                        Some(b'\\') => {
                            if let Some(b'u') = avail(S.pos + 1)? {
                                if let Some(0xFEFF) = read_u(S.pos + 2)? {
                                    S.pos += 6;
                                }
                            }
                        }
                        _ => {}
                    }
                }
                // skip empty lines (\n / \r\n tokens)
                loop {
                    if S.pos >= S.span_end {
                        // no more records
                        return close_case_text(END_TEXT);
                    }
                    if S.pos >= S.win_end && !S.is_last {
                        return Err(E_PAUSE);
                    }
                    if let Some(l) = lf_len(S.pos)? {
                        S.pos += l;
                        S.line += 1;
                        continue;
                    }
                    if let Some(l) = cr_len(S.pos)? {
                        if peek_lf(S.pos + l)? {
                            let l2 = lf_len(S.pos + l)?.unwrap_or(2);
                            S.pos += l + l2;
                            S.line += 1;
                            continue;
                        }
                        break; // lone CR: record starts, stray_cr handled inside
                    }
                    break;
                }
                // snapshot record
                S.record_line = S.line;
                S.record_row = if S.header && !S.columns_done {
                    -1
                } else {
                    S.rows_count as i32
                };
                S.field_idx = 0;
                S.long_pending = false;
                if S.header && !S.columns_done {
                    if !rows_room(1) {
                        return Err(E_TRIP);
                    }
                    S.row_start_out = S.wr;
                    out_b(b'[');
                } else {
                    if !rows_room(2) {
                        return Err(E_TRIP);
                    }
                    S.row_start_out = S.wr;
                    if S.rows_started {
                        out_b(b',');
                    }
                    out_b(b'[');
                }
                S.st = ST_FIELD;
            }
            ST_FIELD => {
                let (is_q, qlen) = peek_quote(S.pos)?;
                if !rows_room(2) {
                    return Err(E_TRIP);
                }
                if S.field_idx > 0 {
                    out_b(b',');
                }
                S.field_start_out = S.wr;
                out_b(b'"');
                S.field_content_start = S.wr;
                if is_q {
                    S.field_quoted = true;
                    S.open = S.pos;
                    S.open_line = S.line;
                    S.open_len = qlen;
                    count_quote_token(qlen);
                    S.pos += qlen;
                    S.st = ST_QUO;
                } else {
                    S.field_quoted = false;
                    S.st = ST_UNQ;
                }
            }
            ST_UNQ => match scan_field(false, false)? {
                END_COMMA => end_field_comma()?,
                e => {
                    if end_field_record(e)? {
                        return Ok(true);
                    }
                    S.st = ST_REC;
                }
            },
            ST_QUO => match scan_field(true, false)? {
                98 => {
                    S.st = ST_AFTER;
                }
                END_TEXT => {
                    // unterminated quote: rewind + re-read as unquoted
                    S.wr = S.field_start_out;
                    S.pos = S.open + S.open_len;
                    S.line = S.open_line;
                    push_err(b"unterminated_quote");
                    if ERR != 0 {
                        return Err(ERR);
                    }
                    if !rows_room(3) {
                        return Err(E_TRIP);
                    }
                    out_b(b'"');
                    S.field_content_start = S.wr;
                    out_str(b"\\\"");
                    S.field_quoted = false;
                    S.st = ST_UNQ;
                }
                e => {
                    if end_field_record(e)? {
                        return Ok(true);
                    }
                    S.st = ST_REC;
                }
            },
            ST_AFTER => {
                if S.pos >= S.span_end {
                    if end_field_record(END_TEXT)? {
                        return Ok(true);
                    }
                    S.st = ST_REC;
                    continue;
                }
                if S.pos >= S.win_end && !S.is_last {
                    return Err(E_PAUSE);
                }
                if let Some(l) = comma_len(S.pos)? {
                    S.pos += l;
                    end_field_comma()?;
                    continue;
                }
                if let Some(l) = lf_len(S.pos)? {
                    S.pos += l;
                    S.line += 1;
                    if end_field_record(END_BREAK)? {
                        return Ok(true);
                    }
                    S.st = ST_REC;
                    continue;
                }
                if let Some(l) = cr_len(S.pos)? {
                    if peek_lf(S.pos + l)? {
                        let l2 = lf_len(S.pos + l)?.unwrap_or(2);
                        S.pos += l + l2;
                        S.line += 1;
                        if end_field_record(END_BREAK)? {
                            return Ok(true);
                        }
                        S.st = ST_REC;
                        continue;
                    }
                    // lone CR after closing quote -> junk drops it
                    S.junk_start_out = S.wr;
                    S.st = ST_JUNK;
                    continue;
                }
                S.junk_start_out = S.wr;
                S.st = ST_JUNK;
            }
            ST_JUNK => match scan_field(false, true)? {
                END_COMMA => {
                    // quote_junk iff the junk kept any character: check
                    // BEFORE close_field writes the closing quote.
                    let kept = S.wr > S.junk_start_out;
                    let _ = close_field()?;
                    if kept {
                        push_err(b"quote_junk");
                        if ERR != 0 {
                            return Err(ERR);
                        }
                    }
                    S.field_idx += 1;
                    S.st = ST_FIELD;
                }
                e => {
                    // Junk fields always had a closing quote, so the
                    // single-empty-unquoted discard can never apply here.
                    let kept = S.wr > S.junk_start_out;
                    let _ = close_field()?;
                    if kept {
                        push_err(b"quote_junk");
                        if ERR != 0 {
                            return Err(ERR);
                        }
                    }
                    if finish_record(e, false)? {
                        return Ok(true);
                    }
                    S.st = ST_REC;
                }
            },
            _ => return Err(E_DECLINE),
        }
    }
}

// ---------- envelope ----------
unsafe fn env_byte() -> Result<u8, u8> {
    if S.pos >= S.win_end {
        if S.is_last {
            return Err(E_DECLINE);
        }
        return Err(E_PAUSE);
    }
    let b = ib(S.pos);
    S.pos += 1;
    Ok(b)
}
// Resume-safe literal matching: exp_start arms the literal once (at a
// sub-state transition); exp_step consumes one byte per call and keeps its
// index in S, so a pause mid-literal resumes instead of restarting.
unsafe fn exp_start(lit: &[u8]) {
    ptr::copy_nonoverlapping(lit.as_ptr(), S.exp_buf.as_mut_ptr(), lit.len());
    S.exp_len = lit.len() as u8;
    S.exp_idx = 0;
}
unsafe fn exp_step() -> Result<bool, u8> {
    if S.exp_idx >= S.exp_len {
        return Ok(true);
    }
    let b = env_byte()?;
    if b != S.exp_buf[S.exp_idx as usize] {
        return Err(E_DECLINE);
    }
    S.exp_idx += 1;
    Ok(S.exp_idx >= S.exp_len)
}

// v128 scan for the span terminator: the first '"' that is NOT escaped.
// A '"' preceded by '\' is part of an escape (\"), not the terminator; a
// '\\' escape consumes its second byte too, so the scan tracks '\' hits
// in order and skips the byte after each one.
unsafe fn span_scan() -> Result<Option<u32>, u8> {
    let mut p = S.scan_pos;
    loop {
        let mut q = p;
        let mut found: Option<(u32, u8)> = None;
        while q + 16 <= S.win_end {
            let v = v128_load(S.in_ptr.add(q as usize) as *const v128);
            let m = i8x16_bitmask(v128_or(
                i8x16_eq(v, i8x16_splat(b'\\' as i8)),
                i8x16_eq(v, i8x16_splat(b'"' as i8)),
            ));
            if m == 0 {
                q += 16;
            } else {
                let i = q + m.trailing_zeros();
                found = Some((i, ib(i)));
                break;
            }
        }
        if found.is_none() {
            while q < S.win_end {
                let b = ib(q);
                if b == b'\\' || b == b'"' {
                    found = Some((q, b));
                    break;
                }
                q += 1;
            }
        }
        match found {
            None => {
                if S.is_last {
                    return Err(E_DECLINE);
                }
                S.scan_pos = S.win_end;
                return Err(E_PAUSE);
            }
            Some((hp, b'\\')) => {
                if hp + 1 >= S.win_end {
                    if S.is_last {
                        return Err(E_DECLINE);
                    }
                    S.scan_pos = hp;
                    return Err(E_PAUSE);
                }
                p = hp + 2;
            }
            Some((hp, _)) => return Ok(Some(hp)),
        }
    }
}

unsafe fn case_init() {
    // S.header was set by PH_HEADER before calling
    S.line = 1;
    S.record_line = 1;
    S.record_row = -1;
    S.width = -1;
    S.field_idx = 0;
    S.rows_count = 0;
    S.rows_started = false;
    S.columns_done = false;
    S.st = ST_REC;
    S.long_pending = false;
    S.errs_count = 0;
    S.wr = 0;
    S.we = 0;
    S.c_2byte = 0;
    S.c_slash = 0;
    S.c_u_print = 0;
    S.c_u_short = 0;
    S.c_u_ctrl = 0;
    S.c_u_bmp = 0;
    S.c_u_pair = 0;
    S.c_u_lone = 0;
    S.c_u_token = 0;
    S.pos = S.span_start;
    // rows prefix
    out_str(b"{\"columns\":");
    if !S.header {
        out_str(b"null,\"rows\":[");
    }
    S.commit_out = S.wr;
    S.phase = PH_CSV;
}

unsafe fn run_phases() -> i32 {
    loop {
        match S.phase {
            PH_ENV0 => {
                // optional BOM (pause-safe even in a tiny first window)
                if S.pos == 0 {
                    match avail(0) {
                        Ok(Some(0xEF)) => {
                            let b1 = match avail(1) {
                                Ok(Some(b)) => b,
                                Ok(_) => return RC_DECLINE,
                                Err(e) => return map_err(e),
                            };
                            let b2 = match avail(2) {
                                Ok(Some(b)) => b,
                                Ok(_) => return RC_DECLINE,
                                Err(e) => return map_err(e),
                            };
                            if b1 == 0xBB && b2 == 0xBF {
                                S.pos += 3;
                            } else {
                                return RC_DECLINE;
                            }
                        }
                        Ok(_) => {}
                        Err(e) => return map_err(e),
                    }
                }
                match env_byte() {
                    Ok(b'[') => {
                        S.phase = PH_ENV_CASE;
                        S.sub = 0;
                    }
                    Ok(_) => return RC_DECLINE,
                    Err(e) => return map_err(e),
                }
            }
            PH_ENV_CASE => match S.sub {
                0 => match env_byte() {
                    Ok(b']') => S.phase = PH_FILE_END,
                    Ok(b'{') => {
                        exp_start(b"\"id\":\"");
                        S.sub = 1;
                    }
                    Ok(_) => return RC_DECLINE,
                    Err(e) => return map_err(e),
                },
                1 => loop {
                    match exp_step() {
                        Ok(true) => {
                            S.sub = 2;
                            break;
                        }
                        Ok(false) => continue,
                        Err(e) => return map_err(e),
                    }
                },
                2 => {
                    // scan id until raw '"'
                    loop {
                        match env_byte() {
                            Ok(b'"') => break,
                            Ok(b'\\') => match env_byte() {
                                Ok(_) => {}
                                Err(e) => return map_err(e),
                            },
                            Ok(_) => {}
                            Err(e) => return map_err(e),
                        }
                    }
                    exp_start(b",\"input\":{\"text\":\"");
                    S.sub = 3;
                }
                _ => loop {
                    match exp_step() {
                        Ok(true) => {
                            S.span_start = S.pos;
                            S.scan_pos = S.pos;
                            S.phase = PH_SPAN_SCAN;
                            S.sub = 0;
                            break;
                        }
                        Ok(false) => continue,
                        Err(e) => return map_err(e),
                    }
                },
            },
            PH_SPAN_SCAN => match span_scan() {
                Ok(Some(q)) => {
                    S.span_end = q;
                    S.pos = q + 1; // consume closing quote
                    S.phase = PH_HEADER;
                    S.sub = 0;
                    exp_start(b",\"header\":");
                }
                Ok(None) => return RC_DECLINE,
                Err(e) => return map_err(e),
            },
            PH_HEADER => match S.sub {
                0 => loop {
                    match exp_step() {
                        Ok(true) => {
                            S.sub = 1;
                            break;
                        }
                        Ok(false) => continue,
                        Err(e) => return map_err(e),
                    }
                },
                1 => match env_byte() {
                    Ok(b't') => {
                        S.header = true;
                        exp_start(b"rue");
                        S.sub = 2;
                    }
                    Ok(b'f') => {
                        S.header = false;
                        exp_start(b"alse");
                        S.sub = 2;
                    }
                    Ok(_) => return RC_DECLINE,
                    Err(e) => return map_err(e),
                },
                2 => loop {
                    match exp_step() {
                        Ok(true) => {
                            exp_start(b"}}");
                            S.sub = 3;
                            break;
                        }
                        Ok(false) => continue,
                        Err(e) => return map_err(e),
                    }
                },
                3 => loop {
                    match exp_step() {
                        Ok(true) => {
                            S.sub = 4;
                            break;
                        }
                        Ok(false) => continue,
                        Err(e) => return map_err(e),
                    }
                },
                _ => match env_byte() {
                    Ok(b',') => {
                        S.file_end_after_case = false;
                        S.env_pos = S.pos;
                        case_init();
                    }
                    Ok(b']') => {
                        S.file_end_after_case = true;
                        S.env_pos = S.pos;
                        case_init();
                    }
                    Ok(_) => return RC_DECLINE,
                    Err(e) => return map_err(e),
                },
            },
            PH_CSV => match parse_case() {
                Ok(true) => {
                    S.cases_done += 1;
                    S.long_pending = false;
                    S.commit_out = S.wr;
                    S.pos = S.env_pos;
                    S.sub = 0;
                    S.phase = if S.file_end_after_case {
                        PH_FILE_END
                    } else {
                        PH_ENV_CASE
                    };
                    return RC_CASE;
                }
                Ok(false) => return RC_PAUSE,
                Err(e) => return map_err(e),
            },
            PH_FILE_END => {
                loop {
                    if S.pos >= S.in_len {
                        return RC_FILE;
                    }
                    if S.pos >= S.win_end {
                        if S.is_last {
                            return RC_DECLINE;
                        }
                        return RC_PAUSE;
                    }
                    let b = ib(S.pos);
                    if b == b' ' || b == b'\n' || b == b'\r' || b == b'\t' {
                        S.pos += 1;
                    } else {
                        return RC_DECLINE;
                    }
                }
            }
            _ => return RC_DECLINE,
        }
    }
}

#[inline(always)]
unsafe fn map_err(e: u8) -> i32 {
    if e == E_PAUSE {
        return RC_PAUSE;
    }
    if e == E_TRIP || ERR == E_TRIP {
        S.tripped = 1;
        return RC_TRIP;
    }
    S.declined_files = 1;
    RC_DECLINE
}

// ---------- exports ----------
#[no_mangle]
pub extern "C" fn reserve_input(n: u32) -> u32 {
    unsafe {
        let mut inv: Vec<u8> = Vec::with_capacity(n as usize);
        inv.set_len(n as usize);
        let ip = inv.as_mut_ptr();
        core::mem::forget(inv);
        let mut rv: Vec<u8> = Vec::with_capacity(ROWS_CAP);
        rv.set_len(ROWS_CAP);
        let rp = rv.as_mut_ptr();
        core::mem::forget(rv);
        let mut ev: Vec<u8> = Vec::with_capacity(ERRS_CAP);
        ev.set_len(ERRS_CAP);
        let ep = ev.as_mut_ptr();
        core::mem::forget(ev);
        S.in_ptr = ip;
        S.in_len = n;
        S.rows_ptr = rp;
        S.errs_ptr = ep;
        S.wr = 0;
        S.we = 0;
        S.pos = 0;
        S.phase = PH_ENV0;
        S.cases_done = 0;
        S.declined_files = 0;
        S.tripped = 0;
        ERR = 0;
        ip as u32
    }
}

#[no_mangle]
pub extern "C" fn run_window(win_end: u32, is_last: u32) -> i32 {
    unsafe {
        S.win_end = win_end;
        S.is_last = is_last != 0;
        ERR = 0;
        let rc = run_phases();
        if ERR == E_TRIP {
            S.tripped = 1;
            return RC_TRIP;
        }
        if rc == RC_DECLINE {
            S.declined_files = 1;
        }
        rc
    }
}

#[no_mangle]
pub extern "C" fn rows_ptr() -> u32 {
    unsafe { S.rows_ptr as u32 }
}
#[no_mangle]
pub extern "C" fn rows_len() -> u32 {
    unsafe { S.wr }
}
#[no_mangle]
pub extern "C" fn rows_commit() -> u32 {
    unsafe {
        if S.long_pending {
            S.row_commit_out
        } else {
            S.commit_out
        }
    }
}
#[no_mangle]
pub extern "C" fn errs_ptr() -> u32 {
    unsafe { S.errs_ptr as u32 }
}
#[no_mangle]
pub extern "C" fn errs_len() -> u32 {
    unsafe { S.we }
}
#[no_mangle]
pub extern "C" fn compact_rows(n: u32) {
    unsafe {
        let n = n.min(S.wr);
        if n == 0 {
            return;
        }
        ptr::copy(
            S.rows_ptr.add(n as usize),
            S.rows_ptr,
            (S.wr - n) as usize,
        );
        S.wr -= n;
        S.commit_out = S.commit_out.saturating_sub(n);
        S.row_commit_out = S.row_commit_out.saturating_sub(n);
        S.field_start_out = S.field_start_out.saturating_sub(n);
        S.field_content_start = S.field_content_start.saturating_sub(n);
        S.junk_start_out = S.junk_start_out.saturating_sub(n);
        S.row_start_out = S.row_start_out.saturating_sub(n);
    }
}
#[no_mangle]
pub extern "C" fn compact_errs(n: u32) {
    unsafe {
        let n = n.min(S.we);
        if n == 0 {
            return;
        }
        ptr::copy(
            S.errs_ptr.add(n as usize),
            S.errs_ptr,
            (S.we - n) as usize,
        );
        S.we -= n;
    }
}
#[no_mangle]
pub extern "C" fn cases_done() -> u32 {
    unsafe { S.cases_done }
}
#[no_mangle]
pub extern "C" fn tripped() -> u32 {
    unsafe { S.tripped }
}
#[no_mangle]
pub extern "C" fn declined() -> u32 {
    unsafe { S.declined_files }
}
#[no_mangle]
pub extern "C" fn counter(i: u32) -> u32 {
    unsafe {
        match i {
            0 => S.c_2byte,
            1 => S.c_slash,
            2 => S.c_u_print,
            3 => S.c_u_short,
            4 => S.c_u_ctrl,
            5 => S.c_u_bmp,
            6 => S.c_u_pair,
            7 => S.c_u_lone,
            8 => S.c_u_token,
            _ => 0,
        }
    }
}
