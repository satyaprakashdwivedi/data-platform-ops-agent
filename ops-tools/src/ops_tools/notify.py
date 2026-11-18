"""Tell people what the agent found: Slack when configured, and always the ops.notification table."""
import logging

import httpx

from .db import Database, as_json
from .settings import Settings

log = logging.getLogger("ops_tools.notify")


def send(db: Database, settings: Settings, title: str, body: str, links: dict | None = None,
         run_id: int | None = None) -> dict:
    links = links or {}
    delivered_to = "log"
    if settings.slack_webhook_url:
        blocks = [{"type": "header", "text": {"type": "plain_text", "text": title[:150]}},
                  {"type": "section", "text": {"type": "mrkdwn", "text": body[:2900]}}]
        if links:
            blocks.append({"type": "actions", "elements": [
                {"type": "button", "text": {"type": "plain_text", "text": label}, "url": url,
                 **({"style": "primary"} if label.lower().startswith("approve") else {})}
                for label, url in links.items()]})
        response = httpx.post(settings.slack_webhook_url, json={"text": title, "blocks": blocks}, timeout=15)
        response.raise_for_status()
        delivered_to = "slack"
    log.info("notification: %s | %s | %s", title, body.replace("\n", " ")[:300], links)
    row = db.fetch_one(
        """INSERT INTO ops.notification (run_id, title, body, links, delivered_to)
           VALUES (%s, %s, %s, %s, %s) RETURNING notification_id""",
        (run_id, title, body, as_json(links), delivered_to))
    return {"notification_id": row["notification_id"], "delivered_to": delivered_to}
