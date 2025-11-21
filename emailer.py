# retail_selector/emailer.py
from __future__ import annotations

import smtplib
from email.message import EmailMessage
from email.utils import formatdate
from pathlib import Path
import asyncio


def send_email_with_attachment_sync(
    smtp_server: str,
    smtp_port: int,
    username: str,
    password: str,
    email_from: str,
    email_to: str,
    subject: str,
    body: str,
    attachment_path: Path,
) -> None:
    if not attachment_path.exists():
        raise FileNotFoundError(f"Attachment not found: {attachment_path}")

    msg = EmailMessage()
    msg["From"] = email_from
    msg["To"] = email_to
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)
    msg.set_content(body)

    file_bytes = attachment_path.read_bytes()
    msg.add_attachment(
        file_bytes,
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=attachment_path.name,
    )

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(username, password)
        server.send_message(msg)

    print(f"📧 Email sent to {email_to} with attachment {attachment_path.name}")


async def send_email_with_attachment_async(
    smtp_server: str,
    smtp_port: int,
    username: str,
    password: str,
    email_from: str,
    email_to: str,
    subject: str,
    body: str,
    attachment_path: Path,
) -> None:
    await asyncio.to_thread(
        send_email_with_attachment_sync,
        smtp_server,
        smtp_port,
        username,
        password,
        email_from,
        email_to,
        subject,
        body,
        attachment_path,
    )
