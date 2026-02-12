"""Main bot runner loop."""
import signal
from time import sleep, time
from datetime import datetime
from pathlib import Path

from src.strategy.signal_generation import SignalGenerator
from src.core.config import load_config
from src.core.utils import add_jitter
from src.ledger.store import Store
from src.ops.logger import setup_logger
from src.collector.market_discovery import discover_markets
from src.collector.orderbook import fetch_orderbooks_for_markets


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
            
            try:
                # Market discovery
                markets = discover_markets(self.config)
                self.logger.info(f"📊 Scanned {len(markets)} markets")

                # Orderbook fetching
                orderbooks = fetch_orderbooks_for_markets(
                    markets,
                    max_tokens=self.config.orderbook.max_tokens_per_cycle,
                    timeout_per_token=self.config.orderbook.timeout_per_token,
                    rate_limit_delay=self.config.orderbook.rate_limit_delay
                )
                self.logger.info(f"📈 Fetched books for {len(orderbooks)} tokens")

                # --- Signal generation (Issue 6 Part 3) ---
                signals = self.signal_generator.generate_signals(cycle_id, orderbooks)

                signals_evaluated_total = len(signals)
                passed_signals = []

                # Persist signals to DB
                for sig in signals:
                    if sig.side == "buy":
                        p_exec = sig.p_implied_exec_buy
                    else:
                        p_exec = sig.p_implied_exec_sell

                    passed_filters = sig.filter_reason is None
                    # TODO: promote to real JSON array later, e.g. json.dumps([...])
                    reasons_json = None if sig.filter_reason is None else sig.filter_reason

                    if passed_filters:
                        passed_signals.append(sig)

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
                    )

                
                # TODO: Edge estimation (Issue 6) - coming next
                
                # For now, just log a successful cycle
                self.logger.info(f"✓ Cycle {cycle_id} completed (stub)")
                
                execution_time = (time() - start_time) * 1000
                store.update_cycle(
                    cycle_id,
                    status='success',
                    execution_time_ms=execution_time,
                    markets_scanned=len(markets),
                    opportunities_found=0,
                    decisions_made=0
                )
                
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
