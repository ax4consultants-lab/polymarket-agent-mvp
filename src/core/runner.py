"""Main bot runner loop."""
import signal
from time import sleep, time
from datetime import datetime
from pathlib import Path

from src.core.config import load_config
from src.core.utils import add_jitter
from src.ledger.store import Store
from src.ops.logger import setup_logger
from src.collector.market_discovery import discover_markets


class BotRunner:
    def __init__(self):
        self.config = load_config()
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
                
                # TODO: Orderbook fetching (Issue 5)
                # TODO: Edge estimation (Issue 6)
                # TODO: Risk checks (Issue 7)
                # TODO: Execution (Issue 8)
                
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
