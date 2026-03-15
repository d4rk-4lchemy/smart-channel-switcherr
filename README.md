# Smart Channel Switcherr

This Dispatcharr plugin delays the first stream-source selection for a request.

It is intended for clients that take a short time to release a slot after a channel stops. The delay gives Dispatcharr time to free the old connection before it chooses the next stream.

## Disclaimer

This was done entirely by AI (Claude/Codex) with me solving some logic traps.

## What it does

- Hooks `apps.proxy.ts_proxy.views.generate_stream_url`
- Delays only the first selection attempt in a request
- Does not delay retries within the same request
- Supports `Smart Delay`

## Smart Delay

When `Smart Delay` is enabled, the plugin only waits if the target channel's first eligible M3U account is already at its concurrent stream limit.

Examples:

- `CH1` on provider A, then switch to `CH2` on provider A: delay applies
- `CH1` on provider A, then switch to `CH3` on provider B: delay does not apply

## Default settings

- `Smart Delay`: enabled
- `Wait before selecting stream source`: `2.0` seconds

## Relationship to Channel Shutdown Delay

This plugin works together with:

- `Proxy Settings -> Channel Shutdown Delay`

Recommended rule:

- Do not set the plugin delay lower than `Channel Shutdown Delay`
- Best results are usually with the plugin delay about `1` second higher

Example:

- `Channel Shutdown Delay = 1s`
- plugin delay = `2s`

That gives Dispatcharr enough time to stop the old channel and then wait a little longer before choosing the next provider stream.

## Notes

- If the previous channel is still active when the delay ends, Dispatcharr may still fall back to another provider
- Increasing the delay can help if your client releases slots slowly
