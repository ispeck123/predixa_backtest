# PREDIXA Futures Option-3 Implementation Report

**Review basis:** current working tree, current source, current tests, and Git history inspected on 2026-09-11. The worktree contains unrelated runtime files, application changes, generated outputs, PID files, a SQLite database, and backup files; those are excluded from this submission. The three relevant historical commits visible in Git are `1acaa6c` (future bucket APIs), `7925f5e` (FYERS APIs), and `73af41b` (GTT flow). The later Option-3 files are currently untracked relative to `HEAD`, so their classification below is based on source ownership and direct usage, not filename assumptions.

## 1. Executive Summary

The repository contains an Option-3 futures path that scans futures setups, persists database-side candidates, requires explicit approval, evaluates the setup through a durable risk engine, creates guarded FYERS entry GTTs, coordinates multiple pending entries using a first-fill winner, continues into the existing futures WebSocket OCO path, and tracks exit/reconciliation state.

The implementation is substantial and test-backed, but the latest focused verification is not fully green. Two tests currently fail: a WebSocket/OCO fixture is missing `expiry_date`, and the final futures broker boundary does not reject a mismatched entry price in one regression test. These are recorded as current limitations; no functionality is claimed as complete solely from the requested design history.

| Capability | Current status |
|---|---|
| Futures scanner persistence | COMPLETE |
| Manual futures approval | COMPLETE |
| Risk evaluation | COMPLETE |
| FYERS futures GTT submission | COMPLETE |
| Multiple pending futures entries | COMPLETE |
| First-fill winner coordination | COMPLETE |
| Loser entry-GTT cancellation | COMPLETE |
| Existing SL/TP OCO continuation | PARTIAL: current test fixture failure |
| Winner exit/completion lifecycle | COMPLETE in focused tests |
| Structured approval responses | COMPLETE in focused tests |
| Full current regression suite | PARTIAL: 153 passed, 2 failed |

## 2. Business Requirement

Option-3 is implemented as a futures-only, BUY/LONG-oriented graduated-live path. Scanner output becomes a manually reviewable candidate. A contract uses the configured one-lot quantity for its instrument. Multiple approved entry GTTs may rest simultaneously before a fill. The first authoritative filled entry is claimed as the winner and other pending futures entry GTTs are cancelled by persisted broker identifiers. Cash execution remains a separate branch, while the shared realized-loss ceiling applies where the RiskEngine uses it.

## 3. Previous Problem / Motivation

The current implementation addresses the separation between setup generation and broker execution, durable reservation of approved submissions, duplicate prevention, exact broker-ID matching, multi-pending futures coordination, explicit recovery, and operator-readable approval outcomes. Existing cash and futures API code also contains global-halt guards and explicit protective-order methods. These changes are visible in the source and tests; unrelated application changes were not included in this package.

## 4. Final Architecture

```text
Futures setup scanner
  -> TradeSignal
  -> Future_Order
  -> OMSOrderBucketFuture (approval_status=pending)
  -> POST /approve/reject/bucket/orders/future
  -> RiskEngine validation
  -> durable reservation
  -> guarded FyersClient futures GTT
  -> multiple pending entry GTTs
  -> FYERS onOrder status == 2
  -> exact persisted reservation/broker-ID match
  -> atomic first-fill winner claim
  -> cancel other pending futures entry GTTs
  -> existing OMSOrderBucketFuture WebSocket OCO flow
  -> exit and EXIT_RECONCILING/IDLE lifecycle
```

### Classification of major work

**Newly implemented:** the dedicated graduated scanner, durable futures state store, Option-3 RiskEngine, execution adapter, futures reconciliation/recovery modules, selector, gate-validation module, structured approval response module, and focused tests.

**Modified/extended:** futures approval API, shared FYERS wrapper, request/model definitions, scanner expiry integration, setup-engine output exposure, WebSocket callback coordination, shared persistence helpers, application startup, and cash boundary guards.

**Existing and reused:** the underlying setup engine and `OMSOrderBucketFuture` SL/TP OCO flow. The WebSocket coordination calls into that existing OCO path rather than creating a second protection architecture.

## 5. Scanner Persistence

`scripts/futures_graduated_live_scanner.py` invokes the existing setup engine and formatter, limits jobs to the configured futures universe, and uses dynamic expiry discovery through `scanner_fc.build_futures_expiry_map`. Accepted BUY setups call the existing `insert_trade_signals` path and then create or reuse one `Future_Order` and one `OMSOrderBucketFuture` linked by `trade_signal_id`.

Fresh OMS candidates are persisted with `approval_status="pending"`, null approver/time, null `gtt_id` and `id_fyers`, and `is_trade_started=0`. The scanner uses `FUTURES_LOT_SIZES` for the configured instruments and rejects missing/invalid quantity instead of fabricating one. It does not call broker mutation methods or create a durable broker reservation. Repeated runs query by `trade_signal_id` and reuse existing rows.

## 6. Manual Approval Workflow

The endpoint is `POST /approve/reject/bucket/orders/future`. The request model accepts `bucket_id`, `approved_by`, and `status`; an example is:

```json
{"bucket_id": 23, "approved_by": "1", "status": "approved"}
```

The API loads the existing `OMSOrderBucketFuture`, validates its persisted side, lifecycle, lot, expiry, contract, and current values, constructs the existing `Signal`, calls `RiskEngine.reserve(..., production=True)`, builds the Single Futures GTT, and submits through `FyersClient.place_futures_entry_gtt`. On success it persists approval metadata and broker IDs without marking the position started. Rejection, edit/reopen, duplicate, deterministic broker rejection, and ambiguous broker outcomes have separate paths and structured responses.

## 7. Risk Management

The RiskEngine checks the current Option-3 policy, reconciliation/readiness, global halt, winner/lifecycle state, approved symbol/contract, lot quantity, signal identity, and LONG relationship. Configured Option-3 values include an INR 18,000 single-trade cap, `PER_LOT_RISK` selection, pool size 8, average-quantity liquidity metric, seven-day lookback, 2% threshold, INR 35,000 margin buffer, 1.5% margin buffer, and four stale-GTT trading days.

The actual fire-time risk is calculated by the engine as:

```text
ABS(entry price - stop-loss price) × quantity
```

The selector separately emits ranking risk based on futures price, lot size, and median stop percentage. The code and response model keep ranking risk separate from actual trade risk.

## 8. ₹50,000 Global Realized-Loss Control

`RiskEngine.refresh` latches the global halt when cash realized loss plus futures realized loss is greater than or equal to `TOTAL_REALIZED_LOSS_CEILING` (50,000). New cash and futures entries are guarded by this state. The source excludes margin, open-risk, and unrealized values from this realized-loss sum. Protective methods are distinct from new-entry methods and remain available during a halt. Durable state stores the loss values and halt; duplicate realized events are tested for idempotency.

## 9. Multiple Pending Futures Orders

The current reservation model permits multiple pre-fill reservations. Each reservation carries signal and contract data and is later linked to its exact entry GTT/broker ID. Before a fill, `serial_reservation` and `active_winner` remain empty. This is the implemented Option-3 model, not the older one-resting-GTT assumption.

## 10. First-Fill-Wins Coordination

`data_fetchers/fyers/fyers_general_socket.py` treats the existing repository event `onOrder` with `orders.status == 2` as the filled/traded event. It accepts only NSE futures-shaped orders and matches them against persisted signal/reservation/broker order identifiers, including GTT and order-tag mappings. Symbol and price alone are not used for ownership.

The callback invokes `GraduatedLiveExecution.claim_futures_winner_and_cancel_losers`, which delegates winner selection to the durable RiskEngine transaction. A duplicate event is idempotent. A competing actual fill is preserved as an exposure breach and locks the durable state.

## 11. Loser GTT Cancellation

After the atomic winner claim, only other reservations classified as pending futures entries are cancellation targets. Cancellation uses each reservation's exact persisted GTT ID. The winner is excluded. Cash GTTs, protective/OCO orders, manual orders, and unrelated futures orders are outside this path. Cancellation timeouts remain unresolved rather than being treated as confirmed cancellation.

## 12. Existing SL/TP OCO Integration

The existing `OMSOrderBucketFuture` WebSocket SL/TP OCO handling is reused/extended. The new coordination is placed before the existing fill branch; it does not introduce a new protective submission API. The current focused WebSocket test suite contains one fixture compatibility failure because its mock row lacks `expiry_date`, which the existing OCO branch accesses.

## 13. Exit and Completion Lifecycle

The RiskEngine supports authoritative winner exit handling, `EXIT_RECONCILING`, terminal loser-cancellation checks, position-closed checks, duplicate exit-event protection, and return to `IDLE` only when unresolved cancellation or exposure conditions are absent. `ERROR_LOCKED` remains sticky for exceptional exposure/reconciliation failures. Focused winner-completion tests cover target and stop outcomes, unresolved losers, duplicate exits, and error-lock protection.

## 14. Durable State Machine

The current configuration and engine use these important states:

| State | Meaning |
|---|---|
| `IDLE` | No active futures execution exposure or pending reservation |
| `ENTRY_RESERVING` | A production approval reservation is being committed |
| `ENTRY_GTT_RESTING` | One or more approved futures entry GTTs are pending |
| `ENTRY_FILL_PENDING_RECONCILIATION` | A winner event was claimed and fill reconciliation remains |
| `POSITION_OPEN_LOCKED` | A futures position/winner is active and new entries are blocked |
| `EXIT_OCO_ACTIVE` | Winner protection is active |
| `EXIT_RECONCILING` | Exit occurred but safe unlock conditions are incomplete |
| `ERROR_LOCKED` | Exceptional or ambiguous exposure/state condition requiring recovery |

## 15. Candidate Rejection / Edit / Reapproval Lifecycle

This lifecycle is implemented for never-submitted/released candidates:

```text
PENDING -> REJECTED -> EDITED/REOPENED -> PENDING -> APPROVED
```

Candidate approval status is separate from global trading state. Unsubmitted user/risk rejection releases any unsubmitted reservation and recomputes normal state. Deterministic broker rejection archives/releases its reservation. Ambiguous broker responses retain unresolved state and do not permit automatic retry. A submitted/resting GTT cannot be edited until its broker cancellation is confirmed.

## 16. Detailed Approval API Response

`apps/backend/api/futures_approval_response.py` centralizes response construction and reason-code messages. Responses include success/decision/reason/message/HTTP status, bucket and signal metadata, order values, calculation values, risk state, broker identifiers where appropriate, evaluated checks, display metadata, and retryable/editable flags. Secrets, tokens, credentials, and raw sensitive broker payloads are excluded.

Successful response shape is equivalent to:

```json
{"success":true,"decision":"SUBMITTED","reason_code":"FUT_ALLOW","message":"Futures entry GTT was approved and submitted successfully.","bucket_id":23,"approval_status":"approved","is_trade_started":0,"broker":{"gtt_id":"...","id_fyers":"..."}}
```

Risk rejection includes the engine's actual `risk_per_unit`, `trade_risk`, open-risk, proposed-risk, configured cap, and realized-loss context when available. A representative rejected code is `FUT_REJECT_TRADE_RISK_GT_CONFIGURED_CAP`; the current message mapper supplies a human-readable message and retry metadata.

## 17. Rejection / Failure Reasons

The following codes are present in current RiskEngine/API paths or response mappings:

| Reason code | Meaning | HTTP convention | Retryability |
|---|---|---:|---|
| `FUT_REJECT_TRADE_RISK_GT_CONFIGURED_CAP` | Actual stop-risk exceeds configured cap | 409 | Yes after edit |
| `GLOBAL_REALIZED_LOSS_CEILING` | Realized-loss halt blocks new entries | 409 | No while latched |
| `FUT_REJECT_INVALID_LOT_SIZE` | Missing/non-integral/non-positive quantity | 422/409 | After correction |
| `FUT_REJECT_SYMBOL_UNRESOLVED` | Contract cannot be resolved safely | 409 | After correction |
| `FUT_REJECT_ACTIVE_FUTURES_WINNER` | Existing futures winner blocks a new entry | 409 | No until lifecycle completes |
| `FUT_REJECT_ENABLEMENT_GATES` | Required configured gates are not approved | 409 | After operator/configuration action |
| `FUT_REJECT_STATE_RECONCILIATION_FAILED` | Durable broker/account state is not safe | 409 | No automatic retry |
| `FUT_REJECT_DUPLICATE_SIGNAL` | Signal is already processed/reserved | 409 | Depends on lifecycle |
| `FYERS_GTT_REJECTED` | Broker definitively rejected with no order | 502 | Retry after cleanup |
| `FYERS_GTT_STATUS_AMBIGUOUS` | Broker outcome cannot be confirmed | 502 | No automatic retry |
| `USER_REJECTED` | User rejected before submission | 200 | Editable/retryable |
| `ALREADY_APPROVED` | Existing broker submission is retained | 200 | No duplicate submission |

## 18. FYERS Integration

The guarded mutation path submits futures entries through `FyersClient.place_futures_entry_gtt`, which ultimately calls the FYERS GTT client only after durable reservation and payload checks. Winner coordination uses the existing cancellation wrapper for exact loser GTT IDs. The WebSocket uses read/event handling and the existing OCO mutation path for protection. Reconciliation/recovery uses read-only positions and GTT orderbook methods. No credentials or tokens are packaged.

## 19. Database/Data Model Impact

The implementation relies on `TradeSignal`, `Future_Order`, and `OMSOrderBucketFuture`, linked with `trade_signal_id` and `order_id`. `OMSOrderBucketFuture` carries candidate approval and broker fields including `approval_status`, `approved_by`, `approved_at`, `gtt_id`, `id_fyers`, `stock_quantity`, prices, expiry, and `is_trade_started`. Durable state is stored in `graduated_live_state`; audit events are stored in `graduated_live_audit`. No database dump or production state snapshot is included.

## 20. Files Implemented / Modified

The complete relevant manifest is in `PREDIXA_Futures_Option3_Submission/FILE_MANIFEST.csv`. It contains 21 production files and 13 focused test files. New Option-3 modules are untracked relative to the inspected `HEAD`; modified files are those shown by current Git status and directly used by the flow. Unrelated changed files such as PID files, runtime data, unrelated endpoints, backup scripts, and generated datasets are excluded.

## 21. Test Coverage

The packaged tests cover scanner persistence and idempotency; approval and edit/retry; risk engine and broker boundary; multiple pending reservations; winner and race behavior; WebSocket identification and OCO continuation; winner completion; snapshot lifecycle; startup/recovery; explicit error recovery; gate/configuration behavior; structured responses; and production halt/protective guards.

## 22. Current Verification

Compilation passed for all 21 packaged production files using Python in conda environment `finpro`.

Focused command:

```bash
PYTHONPATH=. conda run -n finpro pytest -q tests/test_futures_approval.py tests/test_futures_approval_response.py tests/test_futures_scanner_persistence.py tests/test_futures_multi_pending.py tests/test_option3_websocket_winner.py tests/test_option3_winner_completion.py tests/test_futures_snapshot_lifecycle.py tests/test_futures_reconciliation.py tests/test_futures_error_recovery.py tests/test_futures_gate_validation.py tests/test_futures_risk_engine.py tests/test_production_guards.py
```

Result: **153 passed, 2 failed, 14 warnings**.

Current failures:

1. `tests/test_option3_websocket_winner.py::test_matched_futures_fill_continues_existing_oms_oco_flow`: the test mock `OMSOrderBucketFuture` row lacks `expiry_date`, while the current existing OCO branch reads that field.
2. `tests/test_futures_risk_engine.py::test_futures_boundary_rejects_reservation_payload_mismatch[payload_change2]`: the current `FyersClient.place_futures_entry_gtt` boundary did not reject the modified `leg1.price` fixture.

These failures are not hidden or treated as unrelated because they touch the packaged futures path.

## 23. Real Broker Status

No broker mutation was performed while preparing this report. The source contains the production mutation path and tests mock it. Existing user-provided runtime logs indicate successful FYERS submission occurred previously, but this report does not perform or independently revalidate a live order.

## 24. Security / Safety Controls

Current code provides scanner-side broker-mutation prohibition, guarded futures entry submission, durable reservations, exact broker-ID matching, atomic winner handling, fail-closed ambiguous broker outcomes, realized-loss entry halt, cash/futures branch separation, and reuse of the existing OCO path. The current boundary-price regression above is a remaining verification issue and should be reviewed before relying on that specific guard as complete.

## 25. Known Limitations / Remaining Work

The current focused suite is not fully green. The two failures in Section 22 require correction or an explicitly approved test/source compatibility decision. The package also does not include unrelated operator tooling, production state, logs, credentials, database dumps, caches, or live broker evidence. Angular presentation is not included as an implementation change in the inspected relevant files.

## 26. Final Current Status

| Component | Status | Notes |
|---|---|---|
| Scanner persistence | COMPLETE | Creates/reuses three DB-side records for accepted candidates |
| Approval | COMPLETE | Existing futures endpoint and guarded flow |
| Risk evaluation | COMPLETE | Actual stop-risk and lifecycle checks present |
| FYERS GTT | COMPLETE | Guarded entry mutation path present |
| Multiple pending | COMPLETE | Durable reservations coexist before fill |
| First-fill winner | COMPLETE | Durable exact-ID matching and atomic claim tested |
| Loser cancellation | COMPLETE | Exact pending entry GTT IDs; race lock present |
| OCO integration | PARTIAL | Existing path reused; one current fixture fails |
| Exit lifecycle | COMPLETE | Completion/unlock logic covered by focused tests |
| Detailed response | COMPLETE | Structured response tests pass |

## Management Summary

### Achievements

- Added a dedicated futures candidate scanner with database persistence and no broker mutation.
- Added manual approval through existing `OMSOrderBucketFuture` records.
- Added durable Option-3 reservations supporting multiple pending futures GTTs.
- Added first-fill winner selection, exact loser cancellation, race detection, and completion handling.
- Integrated coordination with the existing futures WebSocket/OCO implementation.
- Added structured approval responses containing decision context and retry/edit information.

### Current Operational Flow

```text
scan -> persist candidate -> user approval -> risk decision -> reservation -> FYERS GTT -> fill winner -> cancel other entry GTTs -> existing OCO -> exit/reconcile
```

### Management Review Items

- Review the two failing current tests before declaring the packaged implementation fully verified.
- Specifically review the missing `expiry_date` in the OCO fixture and the missing entry-price rejection at the final futures broker boundary.
- Confirm operational policy for the configured Option-3 gates and live-state recovery outside this package.

### Conclusion

The repository contains a broad, implemented Futures Option-3 execution flow with durable state and focused coverage. Based on the current source and latest test run, it should be classified as **PARTIAL verification**, not fully regression-clean, until the two documented failures are resolved.
