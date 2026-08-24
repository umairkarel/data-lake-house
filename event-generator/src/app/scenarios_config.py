# ---------------------------------------------------------------------------
# scenarios_config.py
# ---------------------------------------------------------------------------
# Central control panel for all real-world scenario injections.
#
# HOW TO USE:
#   1. Set "enabled": True for any scenario you want to study.
#   2. Tune the percentages / counts to control intensity.
#   3. Restart the event-generator container: `make restart`
#
# When ALL scenarios are disabled, the generator produces a clean, normal
# stream of new order events with no artificial anomalies — perfect baseline.
# ---------------------------------------------------------------------------

SCENARIOS = {

    # -------------------------------------------------------------------------
    # LATE EVENTS
    # Concept: Watermarks, allowedLateness, side-output streams
    #
    # Injects events whose event_time is set deliberately in the past.
    # Flink's watermark will have already advanced past their timestamp,
    # so they arrive "late" — great for learning how Flink handles out-of-order
    # data and what happens when late records hit a closed window.
    # -------------------------------------------------------------------------
    "late_events": {
        "enabled": True,
        "pct": 0.25,            # fraction of events to make late (0.25 = 25%)
        "min_delay_sec": 30,    # minimum seconds behind real wall-clock time
        "max_delay_sec": 120,   # maximum seconds behind real wall-clock time
    },

    # -------------------------------------------------------------------------
    # DUPLICATE EVENTS
    # Concept: Deduplication with stateful operators, exactly-once semantics
    #
    # Re-sends a previously emitted event with the same event_id. A downstream
    # Flink job should deduplicate on event_id using a KeyedProcessFunction
    # with a ValueState. Teaches the difference between at-least-once and
    # exactly-once guarantees.
    # -------------------------------------------------------------------------
    "duplicate_events": {
        "enabled": False,
        "pct": 0.02,            # fraction of events to re-send as duplicates (0.02 = 2%)
        "window_size": 50,      # how many recent event_ids to remember for re-sending
    },

    # -------------------------------------------------------------------------
    # ORDER LIFECYCLE (State Machine)
    # Concept: CEP (Complex Event Processing), stateful pattern matching,
    #          order-book joins, temporal patterns
    #
    # Tracks in-flight orders and emits realistic status transitions following
    # the valid state machine:
    #   placed → payment_pending → paid → shipped → delivered
    #                                             ↘ cancelled (rare)
    #                            ↘ cancelled (payment failed)
    #          ↘ cancelled (immediate)
    #                                    delivered → returned (optional)
    #
    # Without this, all events are new "placed" orders. With it, the stream
    # contains the full order lifecycle — great for pattern matching with Flink CEP.
    # -------------------------------------------------------------------------
    "order_lifecycle": {
        "enabled": False,
        "max_inflight_orders": 20,  # how many open orders to track at once
        "lifecycle_pct": 0.30,      # fraction of events that are status updates (not new orders)
        "cancel_pct": 0.10,         # fraction of orders that eventually get cancelled
        "return_pct": 0.05,         # fraction of delivered orders that get returned
    },

    # -------------------------------------------------------------------------
    # FLASH SALE (Traffic Burst)
    # Concept: Session Windows, Tumbling Windows, backpressure, spike handling
    #
    # Every N seconds, emits a large burst of events concentrated on a single
    # product category. Real-world analogy: Black Friday or a time-limited sale.
    # Teaches windowing behavior under bursty traffic and backpressure management.
    # -------------------------------------------------------------------------
    "flash_sale": {
        "enabled": False,
        "interval_sec": 60,     # how often a flash sale is triggered (seconds)
        "burst_count": 40,      # number of extra events to emit in the burst
        "category": None,       # None = random category; or set e.g. "electronics"
    },

    # -------------------------------------------------------------------------
    # HIGH VALUE ORDERS
    # Concept: CEP threshold alerting, fraud detection patterns
    #
    # Forces a percentage of new orders to have a very high total_amount.
    # A downstream Flink job can alert on orders above a threshold.
    # Teaches CEP pattern conditions and side-output alerting.
    # -------------------------------------------------------------------------
    "high_value_orders": {
        "enabled": False,
        "pct": 0.05,            # fraction of new orders that are high-value (0.05 = 5%)
        "min_amount": 1000.0,   # minimum total_amount for these orders
        "max_amount": 9999.0,   # maximum total_amount for these orders
    },

    # -------------------------------------------------------------------------
    # REGIONAL SPIKE
    # Concept: Partitioned aggregations, hot-key problems, data skew
    #
    # Every N seconds, one region gets a disproportionate share of events.
    # Teaches how to write Flink aggregations partitioned by region and how
    # data skew affects parallelism when one key dominates traffic.
    # -------------------------------------------------------------------------
    "regional_spike": {
        "enabled": False,
        "interval_sec": 120,    # how often a regional spike is triggered (seconds)
        "spike_count": 30,      # number of extra events for the spiked region
        "region": None,         # None = random region; or set e.g. "us-east"
    },
}
