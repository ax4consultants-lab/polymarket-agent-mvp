# 1. DocString
"""Main bot runner loop."""
# 2. Standard library imports
import json # ensure this is at the top of the file.
import signal
from time import sleep, time
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field  # ADD THIS
from collections import defaultdict        # ADD THIS

# 3. Project imports
from src.strategy.signal_generation import SignalGenerator
from src.core.config import load_config
from src.core.utils import add_jitter
from src.ledger.store import Store
from src.ops.logger import setup_logger
from src.collector.market_discovery import discover_markets
from src.collector.orderbook import fetch_orderbooks_for_markets

# 4. New: CycleMetrics dataclass
@dataclass
class CycleMetrics:
    """Per-cycle metrics for observability."""
    # Discovery
    markets_scanned: int = 0
    markets_selected: int = 0
    
    # Orderbooks
    orderbooks_attempted: int = 0
    orderbooks_ok: int = 0
    orderbooks_failed: int = 0  # API failures
    invalid_books: dict = field(default_factory=lambda: defaultdict(int))  # reason -> count
    
    # Filters
    filtered_invalid_book: int = 0
    filtered_spread: int = 0
    filtered_depth: int = 0
    filtered_stale: int = 0
    
    # Signals
    signals_generated: int = 0
    signals_passed: int = 0
    
    def summary_line(self) -> str:
        """Generate one-line summary for logging."""
        invalid_breakdown = ", ".join(
            f"{count} {reason}" for reason, count in self.invalid_books.items()
        ) if self.invalid_books else "none"
        
        return (
            f"📊 Markets: {self.markets_selected}/{self.markets_scanned} | "
            f"Books: {self.orderbooks_ok}/{self.orderbooks_attempted} "
            f"(invalid: {invalid_breakdown}) | "
            f"Filtered: {self.filtered_invalid_book} invalid_book, "
            f"{self.filtered_spread} spread, {self.filtered_depth} depth, "
            f"{self.filtered_stale} stale | "
            f"Signals: {self.signals_passed}/{self.signals_generated} passed"
        )


# 5. Existing BotRunner class
class BotRunner:
    def __init__(self):
        self.config = load_config()
        self.signal_generator = SignalGenerator(self.config.signals)
        self.logger = setup_logger(self.config)
        self.store = Store(Path(self.config.bot.database_path))
        self.running = True
        self.error_count = 0
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
    
    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info("🛑 Shutdown signal received")
        self.running = False
    
    def run_cycle(self) -> None:
        """Execute one bot cycle."""
        start_time = time()
        cycle_timestamp = datetime.now().timestamp()
        
        with self.store as store:
            cycle_id = store.create_cycle(cycle_timestamp, 'success')

            metrics = CycleMetrics() # Added 02162026
            
            try:
                # Market discovery
                markets = discover_markets(self.config)
                
                # Track discovery metrics
                metrics.markets_scanned = len(markets)
                metrics.markets_selected = len(markets)
                
                self.logger.info(f"📊 Scanned {len(markets)} markets")

                # Orderbook fetching
                orderbooks = fetch_orderbooks_for_markets(
                    markets,
                    max_tokens=self.config.orderbook.max_tokens_per_cycle,
                    timeout_per_token=self.config.orderbook.timeout_per_token,
                    rate_limit_delay=self.config.orderbook.rate_limit_delay
                )
                
                # Track orderbook metrics
                total_tokens = sum(len(m.tokens) for m in markets)
                metrics.orderbooks_attempted = min(total_tokens, self.config.orderbook.max_tokens_per_cycle)
                metrics.orderbooks_ok = sum(1 for b in orderbooks if b.validity_reason is None)
                metrics.orderbooks_failed = metrics.orderbooks_attempted - len(orderbooks)
                
                # Track invalid books by reason
                for book in orderbooks:
                    if book.validity_reason:
                        metrics.invalid_books[book.validity_reason] += 1
                
                self.logger.info(f"📈 Fetched books for {len(orderbooks)} tokens ({metrics.orderbooks_ok} valid)")

                # --- Signal generation (Issue 6 Part 3) ---
                signals = self.signal_generator.generate_signals(cycle_id, orderbooks)

                # Track signal metrics
                metrics.signals_generated = len(signals)

                signals_evaluated_total = len(signals)
                passed_signals = []

                # Persist signals to DB
                for sig in signals:
                    if sig.side == "buy":
                        p_exec = sig.p_implied_exec_buy
                    else:
                        p_exec = sig.p_implied_exec_sell


                    passed_filters = sig.filter_reason is None

                    if sig.filter_reason is None:
                        reasons_json = None
                    else:
                        reasons_json = json.dumps([sig.filter_reason])


                    if passed_filters:
                        passed_signals.append(sig)

                    else:
                        # Track filter reasons
                        if sig.filter_reason == "spread_too_wide":
                            metrics.filtered_spread += 1
                        elif sig.filter_reason == "depth_too_thin":
                            metrics.filtered_depth += 1
                        elif sig.filter_reason == "snapshot_stale":
                            metrics.filtered_stale += 1
                        elif sig.filter_reason == "invalid_book":
                            metrics.filtered_invalid_book += 1

                    store.record_signal(
                        cycle_id=sig.cycle_id,
                        market_id=sig.market_id,
                        token_id=sig.token_id,
                        side=sig.side,
                        p_implied_mid=sig.p_implied_mid,
                        p_implied_exec=p_exec,
                        p_fair=sig.fair_value_prob,
                        edge_bps=sig.edge_bps,
                        spread_bps=sig.spread_bps,
                        depth_within_1pct=sig.depth_within_1pct,
                        passed_filters=passed_filters,
                        reasons_json=reasons_json,
                    )

                # Update passed signals count
                metrics.signals_passed = len(passed_signals)

                # Logging: distinguish evaluated vs passed
                passed_count = len(passed_signals)

                if passed_count > 0:
                    self.logger.info(
                        f"🎯 Candidates after filters: {passed_count}/{signals_evaluated_total}"
                    )
                    self.logger.info(
                        "🎯 Top signals this cycle: "
                        + ", ".join(
                            f"{s.side.upper()} {s.token_id[:6]} edge={s.edge_bps:.1f}bps"
                            for s in passed_signals[: self.config.signals.top_n_to_log]
                        )
                    )
                else:
                    self.logger.info(
                        f"🎯 Candidates after filters: 0/{signals_evaluated_total} "
                        "(all rejected by filters)"
                    )



                # Record orderbook summaries in DB
                for book in orderbooks:
                    store.record_orderbook_summary(
                        cycle_id=cycle_id,
                        market_id=book.market_id,
                        token_id=book.token_id,
                        best_bid=book.best_bid,
                        best_ask=book.best_ask,
                        mid_price=book.mid_price,
                        spread_bps=book.spread_bps,
                        depth_within_1pct=book.depth_within_1pct,  # Add this
                        validity_reason=book.validity_reason,
                    )

                
                # TODO: Edge estimation (Issue 6) - coming next
                
                # For now, just log a successful cycle
                self.logger.info(f"✓ Cycle {cycle_id} completed (stub)")
                
                execution_time = (time() - start_time) * 1000
                store.update_cycle(
                    cycle_id,
                    status='success',
                    execution_time_ms=execution_time,
                    markets_scanned=metrics.markets_scanned, #changed 02162026
                    opportunities_found=0,
                    decisions_made=0
                )
                
                # Emit consolidated metrics summary
                self.logger.info(metrics.summary_line())

                self.error_count = 0  # Reset on success
                
            except Exception as e:
                self.logger.error(f"❌ Cycle error: {e}", exc_info=True)
                store.update_cycle(
                    cycle_id,
                    status='error',
                    error_message=str(e)
                )
                
                self.error_count += 1
                if self.error_count >= self.config.risk.max_error_burst:
                    self.logger.critical(
                        f"🚨 Max error burst reached ({self.error_count}) - halting"
                    )
                    self.running = False
    
    def run(self) -> None:
        """Main loop with jitter and graceful shutdown."""
        self.logger.info("🚀 Bot starting...")
        self.logger.info(f"📊 Config: {self.config.bot.cycle_interval_seconds}s cycle, "
                        f"jitter ±{self.config.bot.jitter_seconds}s")
        
        while self.running:
            self.run_cycle()
            
            if self.running:
                interval = self.config.bot.cycle_interval_seconds
                jitter = self.config.bot.jitter_seconds
                sleep_time = add_jitter(interval, jitter)
                self.logger.debug(f"💤 Sleeping {sleep_time:.1f}s until next cycle")
                sleep(sleep_time)
        
        self.logger.info("🛑 Bot stopped")


def main():
    """Entry point."""
    runner = BotRunner()
    runner.run()


if __name__ == "__main__":
    main()
