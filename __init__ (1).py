"""mdcap: market-data capture with a market-making mindset.

Layers (each one reproducible from the one before it):

  L0 raw      cryptofeed PcapRecorder: every WebSocket frame and REST response,
              byte-for-byte, with a nanosecond receive timestamp. SOURCE OF TRUTH.
              Never edited, never deleted by agents.
  L1 norm     Parsed from raw by per-venue parsers, keeping every venue field
              (sizes, update ids, sequence numbers, both venue timestamps).
              Partitioned by each row's own receive hour. Rebuildable at will.
  health      Heartbeats from the live process: per-stream message counts,
              last receive time, event-loop lag. How you know the collector
              itself isn't the thing you're measuring.

Nothing derived (mid, microprice, imbalance, resampled grids) is computed
here. Derivations belong in research code, where they can be changed.
"""

__version__ = "0.1.0"
