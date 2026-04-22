// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed_test

// Integration tests for the revision stream wrapper that exercise
// real KV writes alongside a TestLogReader will be added once the
// real LogReader implementation is available. Those tests should:
//
//  - Start a test server and write keys continuously.
//  - Feed the same writes into a TestLogReader via Generate.
//  - Start a rangefeed with WithRevisionStream behind the writes.
//  - Verify catch-up from the revision stream and transition to
//    live KV with no gaps or duplicates.
