# Break Report

Generated 2026-08-04T00:45:33.516737+00:00 — symbols: BTC-USD, ETH-USD

**Total breaks found: 114**

| Break type | Count |
|---|---|
| unmatched_fill | 60 |
| position_drift | 51 |
| sequence_gap | 3 |

## Measured precision / recall against planted ground truth

| Anomaly type | Planted | Found (matching) | Precision | Recall |
|---|---|---|---|---|
| unmatched_fill | 60 | 60 | 1.000 | 1.000 |
| position_drift | 51 | 51 | 1.000 | 1.000 |
| sequence_gap | 53 (events) | 3 (contiguous runs found) | — | — |

sequence_gap is scored by structural comparison, not a shared ID: adjacent or overlapping planted gaps can merge into one contiguous missing run in the underlying data, so found-runs can be slightly below planted-events even at perfect detection.

## Sample breaks

- `unmatched_fill` [BTC-USD] 2026-08-03 21:00:50.740049+00:00 — fill 3738ebc7-4747-4fd5-9b1e-43d4b099283d at 2026-08-03 21:00:50.740049+00:00 has no observed tick within 0:00:05
- `unmatched_fill` [ETH-USD] 2026-08-03 21:00:53.144761+00:00 — fill 24abab2b-8933-47ad-a886-a87450bb2f22 at 2026-08-03 21:00:53.144761+00:00 has no observed tick within 0:00:05
- `unmatched_fill` [BTC-USD] 2026-08-03 21:00:53.152663+00:00 — fill e21b3e7a-4171-4173-82de-d9cd61231bf4 at 2026-08-03 21:00:53.152663+00:00 has no observed tick within 0:00:05
- `unmatched_fill` [BTC-USD] 2026-08-03 21:00:53.291329+00:00 — fill 04d7a1ed-a64d-41a9-bcab-a089a15a6f8a at 2026-08-03 21:00:53.291329+00:00 has no observed tick within 0:00:05
- `unmatched_fill` [BTC-USD] 2026-08-03 21:00:55.551076+00:00 — fill fe3bca2b-1601-4267-9746-f4a1c77664ce at 2026-08-03 21:00:55.551076+00:00 has no observed tick within 0:00:05
- `unmatched_fill` [BTC-USD] 2026-08-03 21:01:04.804921+00:00 — fill 207dbaf4-659e-4f34-b917-1f2dcbee3df2 at 2026-08-03 21:01:04.804921+00:00 has no observed tick within 0:00:05
- `unmatched_fill` [BTC-USD] 2026-08-03 21:01:05.106429+00:00 — fill e8b8a9b4-a19e-490f-898b-1ede546b1236 at 2026-08-03 21:01:05.106429+00:00 has no observed tick within 0:00:05
- `unmatched_fill` [BTC-USD] 2026-08-03 21:01:05.169825+00:00 — fill d813c45f-62eb-43c4-a5a2-fbfdf6cfa3b8 at 2026-08-03 21:01:05.169825+00:00 has no observed tick within 0:00:05
- `unmatched_fill` [BTC-USD] 2026-08-03 21:01:09.991101+00:00 — fill d4f202b4-fa60-4595-9e89-27e38b302f5c at 2026-08-03 21:01:09.991101+00:00 has no observed tick within 0:00:05
- `unmatched_fill` [BTC-USD] 2026-08-03 21:01:10.006856+00:00 — fill 9175148a-946b-4fa3-a000-f74315c44c89 at 2026-08-03 21:01:10.006856+00:00 has no observed tick within 0:00:05
- `unmatched_fill` [ETH-USD] 2026-08-03 21:01:10.131438+00:00 — fill a567948c-15ac-4e35-9abd-431187d0b431 at 2026-08-03 21:01:10.131438+00:00 has no observed tick within 0:00:05
- `unmatched_fill` [BTC-USD] 2026-08-03 21:01:10.168413+00:00 — fill 0efad433-d10f-4d0b-956e-60a8df9271a6 at 2026-08-03 21:01:10.168413+00:00 has no observed tick within 0:00:05
- `unmatched_fill` [BTC-USD] 2026-08-03 21:01:10.239778+00:00 — fill 2380a339-f5c8-4371-8856-729012605da6 at 2026-08-03 21:01:10.239778+00:00 has no observed tick within 0:00:05
- `unmatched_fill` [BTC-USD] 2026-08-03 21:01:10.318150+00:00 — fill e8b986ec-8041-4a11-beea-a65b6b3319d4 at 2026-08-03 21:01:10.318150+00:00 has no observed tick within 0:00:05
- `unmatched_fill` [BTC-USD] 2026-08-03 21:01:10.318150+00:00 — fill be80d3a6-a45c-4e37-9785-e751e47c91c0 at 2026-08-03 21:01:10.318150+00:00 has no observed tick within 0:00:05
