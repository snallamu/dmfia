"""
DMFIA Scheduler - APScheduler cron trigger at 04:00 UTC daily
"""

import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from crewai_agents import MasterOrchestrator, load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DMFIA.Scheduler")


def daily_job():
    logger.info("Scheduled job triggered")
    try:
        orch = MasterOrchestrator()
        report = orch.run_daily()
        logger.info(f"Job complete. Delivery: {report.delivery_status}")
    except Exception as e:
        logger.exception(f"Scheduled job failed: {e}")


if __name__ == "__main__":
    config = load_config()
    hour, minute = config.get("schedule_utc", "04:00").split(":")

    scheduler = BlockingScheduler()
    scheduler.add_job(
        daily_job,
        CronTrigger(hour=int(hour), minute=int(minute), timezone="UTC"),
        id="dmfia_daily",
        name="DMFIA Daily Run",
        replace_existing=True,
    )

    logger.info(f"Scheduler started. Next run at {hour}:{minute} UTC daily.")
    logger.info("Press Ctrl+C to stop.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
