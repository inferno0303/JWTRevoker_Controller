import json


def do_msg_assembly(event, data):
    """
    Assemble the JSON message.

    :param event: Event name (string)
    :param data: Data dictionary (dict)
    :return: JSON string (str)
    """
    try:
        jsonObject = {
            "event": event,
            "data": data
        }
        return json.dumps(jsonObject)
    except (TypeError, ValueError) as e:
        print(f"Error assembling JSON message: {e}")
        return ""


def do_msg_parse(json_str):
    """
    Parse the JSON message.

    :param json_str: JSON string (str)
    :return: Tuple containing event name (str) and data dictionary (dict)
    """
    try:
        jsonObject = json.loads(json_str)

        event = jsonObject.get("event", "")
        data = jsonObject.get("data", {})

        return event, data
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON message: {e}")
        return "", {}
