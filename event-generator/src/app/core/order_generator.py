"""
order_generator.py
------------------
Domain-aware e-commerce order event generator.

Produces realistic order_events payloads following a valid state machine
per order_id. All real-world scenario injections are controlled via
scenarios_config.SCENARIOS — see that file for full documentation.

State machine per order:
    placed → payment_pending → paid → shipped → delivered
                                               ↘ cancelled  (rare post-ship)
                               ↘ cancelled       (payment failed)
           ↘ cancelled         (immediate cancel)
                                      delivered → returned    (optional)

Terminal states: delivered (if not returning), cancelled, returned.
Once an order reaches a terminal state it is removed from the in-flight dict.
"""

import random
import uuid
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Stats tracker — accumulates across the lifetime of the generator instance
# ---------------------------------------------------------------------------
@dataclass
class GeneratorStats:
    total_events: int = 0
    normal_events: int = 0
    late_events: int = 0
    duplicate_events: int = 0
    total_late_delay_sec: float = 0.0
    min_delay_sec: float = float("inf")
    max_delay_sec: float = 0.0
    delay_buckets: Dict[str, int] = field(default_factory=lambda: {
        "0-30s": 0, "30-60s": 0, "60-90s": 0, "90-120s": 0, "120s+": 0
    })
    late_by_region: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def record_normal(self) -> None:
        self.total_events += 1
        self.normal_events += 1

    def record_late(self, delay_sec: float, region: str = "unknown") -> None:
        self.total_events += 1
        self.late_events += 1
        self.total_late_delay_sec += delay_sec
        self.min_delay_sec = min(self.min_delay_sec, delay_sec)
        self.max_delay_sec = max(self.max_delay_sec, delay_sec)
        self.late_by_region[region] += 1
        # Bucket the delay
        if delay_sec < 30:
            self.delay_buckets["0-30s"] += 1
        elif delay_sec < 60:
            self.delay_buckets["30-60s"] += 1
        elif delay_sec < 90:
            self.delay_buckets["60-90s"] += 1
        elif delay_sec < 120:
            self.delay_buckets["90-120s"] += 1
        else:
            self.delay_buckets["120s+"] += 1

    def record_duplicate(self) -> None:
        self.total_events += 1
        self.duplicate_events += 1

    def to_dict(self) -> Dict[str, Any]:
        avg_delay = (
            round(self.total_late_delay_sec / self.late_events, 1)
            if self.late_events > 0 else 0.0
        )
        late_pct = round(self.late_events / self.total_events * 100, 1) if self.total_events > 0 else 0.0
        return {
            "total_events":      self.total_events,
            "normal_events":     self.normal_events,
            "late_events":       self.late_events,
            "duplicate_events":  self.duplicate_events,
            "late_pct":          f"{late_pct}%",
            "late_delay_sec": {
                "avg": avg_delay,
                "min": round(self.min_delay_sec, 1) if self.late_events > 0 else 0.0,
                "max": round(self.max_delay_sec, 1),
            },
            "delay_distribution": self.delay_buckets,
            "late_by_region":     dict(self.late_by_region),
        }

    def reset(self) -> None:
        self.__init__()

from app.scenarios_config import SCENARIOS


# ---------------------------------------------------------------------------
# Fixed product catalog (20 products across 5 categories)
# ---------------------------------------------------------------------------
PRODUCT_CATALOG = [
    # electronics
    {"product_id": "PROD-001", "name": "Wireless Headphones",   "category": "electronics", "base_price": 149.99},
    {"product_id": "PROD-002", "name": "Bluetooth Speaker",     "category": "electronics", "base_price": 79.99},
    {"product_id": "PROD-003", "name": "USB-C Hub",             "category": "electronics", "base_price": 39.99},
    {"product_id": "PROD-004", "name": "Mechanical Keyboard",   "category": "electronics", "base_price": 119.99},
    # clothing
    {"product_id": "PROD-005", "name": "Running Jacket",        "category": "clothing",    "base_price": 89.99},
    {"product_id": "PROD-006", "name": "Slim Fit Chinos",       "category": "clothing",    "base_price": 54.99},
    {"product_id": "PROD-007", "name": "Merino Wool Sweater",   "category": "clothing",    "base_price": 74.99},
    {"product_id": "PROD-008", "name": "Waterproof Boots",      "category": "clothing",    "base_price": 129.99},
    # food
    {"product_id": "PROD-009", "name": "Organic Coffee Beans",  "category": "food",        "base_price": 24.99},
    {"product_id": "PROD-010", "name": "Protein Bar Pack",      "category": "food",        "base_price": 19.99},
    {"product_id": "PROD-011", "name": "Olive Oil (1L)",        "category": "food",        "base_price": 14.99},
    {"product_id": "PROD-012", "name": "Dark Chocolate Box",    "category": "food",        "base_price": 12.99},
    # books
    {"product_id": "PROD-013", "name": "Designing Data-Intensive Apps", "category": "books", "base_price": 49.99},
    {"product_id": "PROD-014", "name": "The Pragmatic Programmer",      "category": "books", "base_price": 44.99},
    {"product_id": "PROD-015", "name": "Clean Code",                    "category": "books", "base_price": 39.99},
    {"product_id": "PROD-016", "name": "System Design Interview",       "category": "books", "base_price": 34.99},
    # sports
    {"product_id": "PROD-017", "name": "Yoga Mat",             "category": "sports", "base_price": 34.99},
    {"product_id": "PROD-018", "name": "Resistance Bands Set", "category": "sports", "base_price": 22.99},
    {"product_id": "PROD-019", "name": "Water Bottle (32oz)",  "category": "sports", "base_price": 19.99},
    {"product_id": "PROD-020", "name": "Running Shoes",        "category": "sports", "base_price": 109.99},
]

# Pool of 100 stable user IDs
USER_POOL = [f"USR-{i:05d}" for i in range(1, 101)]

REGIONS   = ["us-east", "us-west", "eu-central", "ap-south"]
PLATFORMS = ["web", "mobile", "app"]

# Valid transitions: current_status → list of possible next statuses (in order of likelihood)
STATUS_TRANSITIONS = {
    "placed":          ["payment_pending", "cancelled"],
    "payment_pending": ["paid",            "cancelled"],
    "paid":            ["shipped"],
    "shipped":         ["delivered",       "cancelled"],
    "delivered":       ["returned"],        # only terminal if return not chosen
}
TERMINAL_STATUSES = {"delivered", "cancelled", "returned"}


class OrderEventGenerator:
    """
    Generates realistic e-commerce order_events payloads.

    Each call to generate_event() returns exactly one dict that is ready
    to be serialised to JSON and published to Kafka.
    """

    def __init__(self) -> None:
        # in-flight orders: order_id → order snapshot dict
        self._inflight: Dict[str, Dict[str, Any]] = {}

        # rolling buffer of recent event_ids for duplicate injection
        self._recent_event_ids: deque = deque(maxlen=100)

        # buffer for holding events that will be emitted out of order
        self._out_of_order_buffer: deque = deque()

        # flash-sale timer
        self._last_flash_sale: Optional[datetime] = None

        # regional-spike timer
        self._last_regional_spike: Optional[datetime] = None
        self._spike_region: Optional[str] = None
        self._spike_remaining: int = 0

        # flash-sale burst state
        self._flash_category: Optional[str] = None
        self._flash_remaining: int = 0

        # Stats tracker — accumulates across the lifetime of this generator
        self.stats: GeneratorStats = GeneratorStats()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_event(self) -> Dict[str, Any]:
        """Return one order_events payload, applying active scenarios."""
        # 0. Out-of-order event emission
        if self._out_of_order_buffer and random.random() < 0.3:
            payload = self._out_of_order_buffer.popleft()
            # Still count this as a normal event generation for stats purposes
            self.stats.record_normal()
            return payload

        # 1. Duplicate injection (uses a previously emitted event_id)
        dup_cfg = SCENARIOS["duplicate_events"]
        if (
            dup_cfg["enabled"]
            and self._recent_event_ids
            and random.random() < dup_cfg["pct"]
        ):
            self.stats.record_duplicate()
            return self._build_duplicate()

        # 2. Flash-sale burst
        self._maybe_trigger_flash_sale()
        if self._flash_remaining > 0:
            self._flash_remaining -= 1
            return self._build_new_order(force_category=self._flash_category)

        # 3. Regional spike burst
        self._maybe_trigger_regional_spike()
        if self._spike_remaining > 0:
            self._spike_remaining -= 1
            return self._build_new_order(force_region=self._spike_region)

        # 4. Lifecycle update vs new order
        lc_cfg = SCENARIOS["order_lifecycle"]
        if (
            lc_cfg["enabled"]
            and self._inflight
            and random.random() < lc_cfg["lifecycle_pct"]
        ):
            return self._build_lifecycle_event()

        # 5. Default: brand-new order
        return self._build_new_order()

    def generate_batch(self, n: int) -> List[Dict[str, Any]]:
        """Return a batch of n events."""
        return [self.generate_event() for _ in range(n)]

    # ------------------------------------------------------------------
    # Internal builders
    # ------------------------------------------------------------------

    def _build_new_order(
        self,
        force_category: Optional[str] = None,
        force_region: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a fresh order in 'placed' status."""
        product = self._pick_product(force_category)
        user_id = random.choice(USER_POOL)
        quantity = random.randint(1, 5)

        hv_cfg = SCENARIOS["high_value_orders"]
        if hv_cfg["enabled"] and random.random() < hv_cfg["pct"]:
            total = round(random.uniform(hv_cfg["min_amount"], hv_cfg["max_amount"]), 2)
            unit_price = round(total / quantity, 2)
            discount_pct = 0.0
        else:
            discount_pct = round(random.choice([0.0, 0.0, 0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.50]), 2)
            unit_price   = round(product["base_price"] * (1 - discount_pct), 2)
            total        = round(unit_price * quantity, 2)

        order_id = f"ORD-{uuid.uuid4().hex[:7].upper()}"
        event_id = str(uuid.uuid4())
        region   = force_region or random.choice(REGIONS)
        event_time = self._make_event_time(region=region)

        payload = {
            "event_id":    event_id,
            "order_id":    order_id,
            "user_id":     user_id,
            "product_id":  product["product_id"],
            "category":    product["category"],
            "status":      "placed",
            "quantity":    quantity,
            "unit_price":  unit_price,
            "total_amount": total,
            "discount_pct": discount_pct,
            "region":      region,
            "platform":    random.choice(PLATFORMS),
            "event_time":  event_time,
        }

        # Track in-flight for lifecycle scenario
        lc_cfg = SCENARIOS["order_lifecycle"]
        if lc_cfg["enabled"]:
            if len(self._inflight) < lc_cfg["max_inflight_orders"]:
                self._inflight[order_id] = {
                    "status":      "placed",
                    "user_id":     user_id,
                    "product_id":  product["product_id"],
                    "category":    product["category"],
                    "quantity":    quantity,
                    "unit_price":  unit_price,
                    "total_amount": total,
                    "discount_pct": discount_pct,
                    "region":      payload["region"],
                    "platform":    payload["platform"],
                }

        self._record_event_id(event_id)
        return payload

    def _build_lifecycle_event(self) -> Dict[str, Any]:
        """Advance one in-flight order to the next valid status."""
        lc_cfg = SCENARIOS["order_lifecycle"]
        order_id = random.choice(list(self._inflight.keys()))
        order    = self._inflight[order_id]
        current  = order["status"]

        if current not in STATUS_TRANSITIONS:
            # Already at a pre-terminal; remove and generate a new order instead
            del self._inflight[order_id]
            return self._build_new_order()

        possible = STATUS_TRANSITIONS[current]

        # Apply cancel/return probabilities
        if current == "delivered":
            next_status = "returned" if random.random() < lc_cfg["return_pct"] else None
            if next_status is None:
                # Order lifecycle naturally complete, no more events
                del self._inflight[order_id]
                return self._build_new_order()
        elif "cancelled" in possible and random.random() < lc_cfg["cancel_pct"]:
            next_status = "cancelled"
        else:
            next_status = possible[0]  # always the "happy path" first element

        # Check out-of-order scenario
        ooo_cfg = lc_cfg.get("out_of_order_events", {"enabled": False})
        if ooo_cfg["enabled"] and random.random() < ooo_cfg["pct"]:
            # We want to emit the NEXT state, but buffer THIS state.
            # Example: current=placed. next_status=payment_pending. 
            # We buffer payment_pending, and immediately jump to paid and emit it.
            
            # 1. Build the payload for the current next_status (e.g. payment_pending)
            buffered_event_id = str(uuid.uuid4())
            buffered_event_time = self._make_event_time(region=order["region"])
            
            buffered_payload = {
                "event_id":    buffered_event_id,
                "order_id":    order_id,
                "user_id":     order["user_id"],
                "product_id":  order["product_id"],
                "category":    order["category"],
                "status":      next_status,
                "quantity":    order["quantity"],
                "unit_price":  order["unit_price"],
                "total_amount": order["total_amount"],
                "discount_pct": order["discount_pct"],
                "region":      order["region"],
                "platform":    order["platform"],
                "event_time":  buffered_event_time,
            }
            self._out_of_order_buffer.append(buffered_payload)
            
            # 2. Update internal state to next_status
            order["status"] = next_status
            if next_status in TERMINAL_STATUSES:
                # If the buffered event was terminal, we can't advance further.
                # Just return a new order to avoid breaking things.
                del self._inflight[order_id]
                return self._build_new_order()
                
            # 3. Determine the status AFTER next_status (e.g. paid)
            future_possible = STATUS_TRANSITIONS[next_status]
            future_status = future_possible[0] # happy path
            
            # 4. Generate the payload for the future status
            order["status"] = future_status
            if future_status in TERMINAL_STATUSES:
                del self._inflight[order_id]
                
            future_event_id = str(uuid.uuid4())
            # Use a slightly later event time so the stream sees out-of-order timestamps
            # e.g., buffered event = 10:00:00, future event = 10:00:02
            future_event_time = (
                datetime.strptime(buffered_event_time, "%Y-%m-%d %H:%M:%S.%f") 
                + timedelta(seconds=random.uniform(1, 5))
            ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            
            future_payload = {
                "event_id":    future_event_id,
                "order_id":    order_id,
                "user_id":     order["user_id"],
                "product_id":  order["product_id"],
                "category":    order["category"],
                "status":      future_status,
                "quantity":    order["quantity"],
                "unit_price":  order["unit_price"],
                "total_amount": order["total_amount"],
                "discount_pct": order["discount_pct"],
                "region":      order["region"],
                "platform":    order["platform"],
                "event_time":  future_event_time,
            }
            
            self._record_event_id(buffered_event_id)
            self._record_event_id(future_event_id)
            return future_payload

        order["status"] = next_status
        if next_status in TERMINAL_STATUSES:
            del self._inflight[order_id]

        event_id   = str(uuid.uuid4())
        event_time = self._make_event_time(region=order["region"])

        payload = {
            "event_id":    event_id,
            "order_id":    order_id,
            "user_id":     order["user_id"],
            "product_id":  order["product_id"],
            "category":    order["category"],
            "status":      next_status,
            "quantity":    order["quantity"],
            "unit_price":  order["unit_price"],
            "total_amount": order["total_amount"],
            "discount_pct": order["discount_pct"],
            "region":      order["region"],
            "platform":    order["platform"],
            "event_time":  event_time,
        }

        self._record_event_id(event_id)
        return payload

    def _build_duplicate(self) -> Dict[str, Any]:
        """Re-emit a previously seen event_id (same id, fresh timestamp)."""
        dup_event_id = random.choice(list(self._recent_event_ids))
        # Build a fresh new-order payload but overwrite event_id so Flink sees it as a dup
        payload = self._build_new_order()
        payload["event_id"] = dup_event_id
        return payload

    # ------------------------------------------------------------------
    # Scenario triggers
    # ------------------------------------------------------------------

    def _maybe_trigger_flash_sale(self) -> None:
        cfg = SCENARIOS["flash_sale"]
        if not cfg["enabled"] or self._flash_remaining > 0:
            return
        now = datetime.utcnow()
        if self._last_flash_sale is None or (
            now - self._last_flash_sale
        ).total_seconds() >= cfg["interval_sec"]:
            self._last_flash_sale  = now
            self._flash_remaining  = cfg["burst_count"]
            self._flash_category   = cfg["category"] or random.choice(
                list({p["category"] for p in PRODUCT_CATALOG})
            )

    def _maybe_trigger_regional_spike(self) -> None:
        cfg = SCENARIOS["regional_spike"]
        if not cfg["enabled"] or self._spike_remaining > 0:
            return
        now = datetime.utcnow()
        if self._last_regional_spike is None or (
            now - self._last_regional_spike
        ).total_seconds() >= cfg["interval_sec"]:
            self._last_regional_spike = now
            self._spike_remaining     = cfg["spike_count"]
            self._spike_region        = cfg["region"] or random.choice(REGIONS)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _pick_product(self, force_category: Optional[str] = None):
        if force_category:
            pool = [p for p in PRODUCT_CATALOG if p["category"] == force_category]
            return random.choice(pool) if pool else random.choice(PRODUCT_CATALOG)
        return random.choice(PRODUCT_CATALOG)

    def _make_event_time(self, region: str = "unknown") -> str:
        """Return an ISO8601 event_time, possibly in the past for late-event scenario."""
        cfg = SCENARIOS["late_events"]
        if cfg["enabled"] and random.random() < cfg["pct"]:
            delay = random.uniform(cfg["min_delay_sec"], cfg["max_delay_sec"])
            ts = datetime.utcnow() - timedelta(seconds=delay)
            self.stats.record_late(delay_sec=delay, region=region)
        else:
            ts = datetime.utcnow()
            self.stats.record_normal()
        return ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _record_event_id(self, event_id: str) -> None:
        """Push to recent-ids buffer (used for duplicate injection)."""
        self._recent_event_ids.append(event_id)
