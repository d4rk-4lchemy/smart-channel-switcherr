# Smart Channel Switcherr

A Dispatcharr plugin that delays stream source selection so that a recently-released provider slot has time to free before the next channel assignment runs.

## Disclaimer

Developed with AI assistance (Codex).

## Installation

Copy this directory to `/data/plugins/smart-channel-switcherr/` and enable the plugin in Dispatcharr.

Or import downloaded ZIP file using Dispatcharr "Plugins" tab.

## How it works

The plugin patches `live_proxy` stream selection to insert a configurable sleep before the first stream source selection in each request. Retries within the same request are never delayed.

## Settings

| Setting | Default | Description |
|---|---|---|
| Wait before selecting stream source | `2.0 s` | How long to sleep before source selection. Set to `0` to disable. |
| Smart Delay | `enabled` | Only apply the delay when the target provider is already at its connection limit for the same client (see below). |

## Smart Delay

When Smart Delay is **disabled**, the wait is always applied on every channel switch.

When Smart Delay is **enabled**, the plugin inspects the target channel's first eligible M3U account and checks how many channels are currently using it. The delay is only applied when that account is at or over its `max_streams` limit and the requesting client already has an active connection on that same account.

The client match uses the same values shown in the Dispatcharr UI: `IP Address` and `User Agent`. If the request comes from a different client, the delay is skipped because no provider slot will be released by that viewer.

The active connection count is read from the same live channel metadata that powers `/proxy/live/status` — not a cached counter — so it reflects actual proxy state at the moment of the request.

### Decision logic

```
1. Resolve the target channel's first eligible stream and its M3U account.
2. If the account has no stream limit (max_streams = 0)  -> skip delay.
3. Count channels currently active on that account.
4. If active count < limit                               -> skip delay.
5. If no active connection matches the same IP + UA      -> skip delay.
6. Otherwise                                             -> apply delay.
```

### Examples

| Case | Setup | CH2 preferred provider | Provider state | Delay? | Dispatcharr behaviour |
|---|---|---|---|---|---|
| 1 | CH1 on provider A (`max_streams=1`) | Provider A | Full (1/1) | Yes | Waits; CH2 claims provider A once CH1 releases its slot |
| 2 | CH1 on provider A (`max_streams=2`) | Provider A | Has capacity (1/2) | No | CH2 starts immediately on provider A |
| 3 | CH1 on provider A | Provider B | Has capacity | No | CH2 starts immediately on provider B; provider A is irrelevant |
| 4 | CH1 + CH2 on provider A (`max_streams=2`) | Provider A | Full (2/2) | Yes | Waits; CH3 claims provider A once either viewer releases their slot |
| 5 | Another device is using provider A | Provider A | Full | No | A new client gets no delay because it will not release that existing slot |

## Relationship to Channel Shutdown Delay

This plugin is designed to work alongside **Proxy Settings > Channel Shutdown Delay**.

The shutdown delay controls how long Dispatcharr keeps the old channel alive after a client disconnects. This plugin's wait runs *after* that — it is the extra buffer between the slot being released and Dispatcharr picking the next stream.

Recommended setup:

```
Channel Shutdown Delay  =  1 s
Plugin wait             =  2 s   (shutdown delay + ~1 s buffer)
```

Setting the plugin wait lower than the shutdown delay means source selection may run before the old slot has actually freed.

## Notes

- If the previous channel is still active when the delay ends, Dispatcharr may still fall back to a different provider. Increase the wait if your client releases slots slowly.
- Smart Delay reads live proxy metadata on every request. On systems with many active channels the scan adds a small overhead; this is negligible for typical home use.
