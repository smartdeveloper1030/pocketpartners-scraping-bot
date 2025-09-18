import requests

logger = None


def format_currency(value: float) -> str:
    # Format number with comma separators
    formatted_num = "{:,.2f}".format(abs(value))
    if value > 0:
        return "+${:}".format(formatted_num)
    return "-${:}".format(formatted_num) if value < 0 else "${:}".format(formatted_num)


def format_change(value: float) -> str:
    formatted_num = "{:,.2f}".format(abs(value))
    if value > 0:
        return "+{:}".format(formatted_num)
    return "-{:}".format(formatted_num)


def format_percentage(value: float) -> str:
    return "{:,.2f}%".format(value).strip()


def format_percentage_change(value: float) -> str:
    formatted_num = "{:,.2f}".format(abs(value))
    if value > 0:
        return "+{:}%".format(formatted_num)
    return "-{:}%".format(formatted_num)


mapping = {
    "deposits": ["📥 Deposits: $%s", "⬅️ Previous: $%s", "🔀 Difference: %s"],
    "withdrawals": ["📤 Withdrawals: $%s", "⬅️ Previous: $%s", "🔀 Difference: %s"],
    "commission": ["🔘 Commission: $%s", "⬅️ Previous: $%s", "🔀 Difference: %s"],
    "balance": ["💵 Balance: $%s", "⬅️ Previous: $%s", "🔀 Difference: %s"],
    "bonus": ["🪙 Bonus: $%s", "⬅️ Previous: $%s", "🔀 Difference: %s"],
    "bottom": ["👥 Visitors: %s", "🗂 Registrations: %s", "🗂 Average: %s", "📥 FTDs: %s", "📥 Average FTDs: %s"],
}


def formatted_message(value_type: str, *args) -> str:
    mapped_lines = mapping[value_type]
    if value_type == "bottom":
        return "\n".join(mapped_lines) % (
            "{:,.0f} ({:,})".format(args[0], args[5]),  # visitors (whole number)
            "{:,.0f} ({:,})".format(args[1], args[6]),  # registrations (whole number)
            format_percentage(args[2]) + f" ({format_percentage_change(args[7])})",
            "{:,.0f} ({:,})".format(args[3], args[8]),  # FTDs (whole number)
            format_percentage(args[4]) + f" ({format_percentage_change(args[9])})"
        )
    else:
        if not args[1]:
            return None
        return "\n".join(mapped_lines) % (
            "{:,.2f} ({})".format(args[2], format_currency(args[1])),
            "{:,.2f}".format(float(args[0])),
            format_currency(args[3]),
        )
        
def formatted_message_even_no_change(value_type: str, *args) -> str:
    mapped_lines = mapping[value_type]
    if value_type == "bottom":
        return "\n".join(mapped_lines) % (
            "{:,.0f} ({:,})".format(args[0], args[5]),  # visitors (whole number)
            "{:,.0f} ({:,})".format(args[1], args[6]),  # registrations (whole number)
            format_percentage(args[2]) + f" ({format_percentage_change(args[7])})",
            "{:,.0f} ({:,})".format(args[3], args[8]),  # FTDs (whole number)
            format_percentage(args[4]) + f" ({format_percentage_change(args[9])})"
        )
    else:
        return "\n".join(mapped_lines) % (
            "{:,.2f} ({})".format(args[2], format_currency(args[1])),
            "{:,.2f}".format(float(args[0])),
            format_currency(args[3]),
        )


def formatted_message_compare(value_type: str, *args) -> str:
    mapped_lines = mapping[value_type]
    if value_type == "bottom":
        return "\n".join(mapped_lines) % (
            args[0], args[1], format_percentage(args[2]),
            args[3], format_percentage(args[4])
        )
    else:
        return "\n".join(mapped_lines) % (
            args[2], format_currency(args[1]), args[0]
        )


def formatted_message_current(value_type: str, *args) -> str:
    mapped_lines = mapping[value_type]
    if value_type == "bottom":
        return "\n" + "\n".join(mapped_lines) % (
            args[0], args[1], format_percentage(args[2]),
            args[3], format_percentage(args[4])
        )
    else:
        return mapped_lines[0] % args[2]


def send_message(bot_token: str, chat_id: str, message: str) -> None:
    try:
        res = requests.get(
            url="https://api.telegram.org/bot%s/sendMessage" % bot_token,
            params={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "MarkdownV2",
            }
        )
    except Exception as e:
        logger.exception(
            "ERR_SEND_MESSAGE -> Chat ID: %s| Error: %s" % (chat_id, message))
    else:
        res_json = res.json()
        if "error_code" in res_json:
            logger.debug("WARN_SEND_MESSAGE -> Code: %s | Description: %s" % (
                res_json["error_code"], res_json["description"]
            ))
        else:
            logger.debug("ALERT REQUEST SENT -> %s" % chat_id)
        return res_json


if __name__ == "__main__":
    import core
    core.chat_ids = core.load_chatids()
    logger = core.logger
    logger.debug("Alert.py initiated as main")

    messages = core.load_messages()
    if messages:
        chat_ids = core.load_chatids()
        # In case of failure during loading latest chatids for unknown reason,
        # it will use the previously loaded chatids in starting of the script
        if not chat_ids:
            chat_ids = core.chat_ids

        for chat_id in chat_ids:
            for message in messages:
                _ = send_message(
                    bot_token=core.bot_token,
                    chat_id=chat_id,
                    message=core.fix_message_format(message)
                )
    else:
        logger.debug("No reports were processed!!")
