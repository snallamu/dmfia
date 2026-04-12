"""
DMFIA Scheduler - APScheduler cron trigger in EST timezone
"""

import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from crewai_agents import MasterOrchestrator, load_config

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
    schedule_str = config.get("schedule_est", config.get("schedule_utc", "08:00"))
    hour, minute = schedule_str.split(":")

    scheduler = BlockingScheduler()
    scheduler.add_job(
        daily_job,
        CronTrigger(hour=int(hour), minute=int(minute), timezone="US/Eastern"),
        id="dmfia_daily",
        name="DMFIA Daily Run",
        replace_existing=True,
    )

    logger.info(f"Scheduler started. Next run at {hour}:{minute} EST daily.")
    logger.info("Press Ctrl+C to stop.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
