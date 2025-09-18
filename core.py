import logging
import pyotp
import json
import requests
import httpx
import time

from datetime import date
from pathlib import Path
from dotenv import dotenv_values
from python_anticaptcha import AnticaptchaClient, NoCaptchaTaskProxylessTask

project_name = "pocketoption"

script_path = Path.cwd()
cookies_path = script_path.joinpath("cookies.json")
messages_path = script_path.joinpath("messages.txt")
credentials_path = script_path.joinpath("credentials.env")
logs_path = script_path.joinpath("logs")
logs_path.mkdir(exist_ok=True)


# Logging Based
logger = logging.Logger(project_name)
logger.setLevel(logging.DEBUG)
file_hander = logging.FileHandler(
    filename=logs_path.joinpath("logs_%s.log" % str(date.today())),
    mode="a"
)
file_hander.setLevel(logging.DEBUG)
file_hander.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(logging.Formatter(
    "%(asctime)s | %(message)s", "%H:%M:%S"))


logger.addHandler(stream_handler)
logger.addHandler(file_hander)

# Loading .env credentials
config = dotenv_values(credentials_path.as_posix())
bot_token = config["bot_token"]
email = config["email"]
password = config["password"]
google_auth_secret_key = config.get("google_auth_secret_key")
anticaptcha_api_key = config.get("anticaptcha_api_key")

client = AnticaptchaClient(anticaptcha_api_key)


# Required Links
# home_link = logged_in_link = "https://affiliate.pocketoption.com/en/dashboard"
# login_link = "https://affiliate.pocketoption.com/en/login"
# otp_verify_link = "https://affiliate.pocketoption.com/en/otp-verify"
# # otp_verify_link = "https://pocketpartners.com/en/api/otp-verify"
# statistics_link = "https://affiliate.pocketoption.com/en/statistics/brief"
# statistics_current_week_link = "https://affiliate.pocketoption.com/en/statistics/brief/currentWeek"
# payment_request_link = "https://affiliate.pocketoption.com/en/payments/request"
# payment_history_link = "https://affiliate.pocketoption.com/en/payments/history"
# top_10_affiliates_link = "https://affiliate.pocketoption.com/en/ratings/top"

home_link = logged_in_link = "https://pocketpartners.com/en/dashboard"
login_link = "https://pocketpartners.com/en/api/login"
otp_verify_link = "https://pocketpartners.com/en/api/otp-verify"
statistics_link = "https://pocketpartners.com/en/statistics/brief"
statistics_current_week_link = "https://pocketpartners.com/en/statistics/brief/currentWeek"
payment_request_link = "https://pocketpartners.com/en/payments/request"
payment_history_link = "https://pocketpartners.com/en/payments/history"
top_10_affiliates_link = "https://pocketpartners.com/en/ratings/top"

session: httpx.AsyncClient = None
cookies = {}

# Loading Target Chat ids for Telegram alerts
chat_ids = []


def get_auth_code() -> str:
    if google_auth_secret_key:
        return pyotp.TOTP(google_auth_secret_key).now()


def load_chatids() -> list[str]:
    try:
        return [
            line.strip()
            for line in open("chat_ids.txt", "r").read().split("\n")
            if line.strip()
        ]
    except Exception as e:
        logger.exception("ERR_LOAD_CHATIDS: %s" % e)


def save_cookies(s: httpx.AsyncClient) -> None:
    try:
        cookies = dict(s.cookies)
        with open(cookies_path.as_posix(), "w") as f:
            json.dump(cookies, f)
    except httpx.CookieConflict:
        pass
    except Exception as e:
        logger.exception("ERR_SAVE_COOKIES: %s" % e)
    else:
        logger.debug("Cookies Saved!!")


def load_cookies() -> dict:
    try:
        with cookies_path.open("r") as f:
            return json.load(f)
    except Exception as e:
        logger.exception("ERR_LOADING_OLD_SESSION: %s" % e)
    return {}


def save_messages(messages: list[str]) -> None:
    try:
        with open(messages_path.as_posix(), "w", encoding="utf-8") as f:
            f.write("\n\n\n\n\n\n\n\n\n\n".join(messages))
    except Exception as e:
        logger.exception("ERR_SAVE_MESSAGES: %s" % e)


def load_messages() -> list[str]:
    try:
        if not messages_path.exists():
            return []

        with messages_path.open("r", encoding="utf-8") as f:
            return f.read().split("\n\n\n\n\n\n\n\n\n\n")
    except Exception as e:
        logger.exception("ERR_LOADING_MESSAGES: %s" % e)


def fix_message_format(value: str) -> str:
    # return value.replace('.', '\\.').replace('[', '\\[').replace(']', '\\]').replace('+', '\\+').replace('-', '\\-')
    return value.replace("$+", "+$").replace("$-", "-$").strip()


base_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}

report_headers = {
    "Referer": "https://pocketpartners.com/en/dashboard",
    "X-Requested-With": "XMLHttpRequest"
}

def delete_cookies() -> None:
    """Delete the cookies.json file to remove all saved cookies."""
    try:
        if cookies_path.exists():
            cookies_path.unlink()
            logger.debug("Cookies file deleted!")
        else:
            logger.debug("Cookies file does not exist.")
    except Exception as e:
        logger.exception(f"ERR_DELETE_COOKIES: {e}")


if __name__ == "__main__":
    while True:
        print(get_auth_code())
        time.sleep(1)
